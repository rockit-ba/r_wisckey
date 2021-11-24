//! 日志存储实现 引擎核心

use std::fs::{File, OpenOptions, remove_file};
use std::collections::btree_map::BTreeMap;
use std::{env, fs, thread};
use std::path::{PathBuf, Path};
use std::ffi::OsStr;
use std::io::{BufReader, Read, BufWriter, Write};
use std::collections::HashMap;

use crate::{KvsEngine};
use crate::engines::record::{RECORD_HEADER_SIZE, RecordHeader, KVPair, CommandType};
use crate::engines::Scans;
use crate::common::fn_util::{checksum, is_eof_err};
use crate::common::types::ByteVec;
use crate::common::error_enum::WiscError;
use crate::config::SERVER_CONFIG;

use anyhow::Result;
use log::info;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use crate::engines::compress::compress;
use std::sync::{Arc, Mutex};
use crate::engines::persistence::{data_log_append, DataAppend, flush};
use std::ops::DerefMut;
use std::thread::sleep;
use std::time::Duration;

/// 存储引擎
#[derive(Debug)]
pub struct LogEngine {
    readers: Arc<Mutex<HashMap<u64,BufReader<File>>>>,
    writer: Arc<Mutex<BufWriter<File>>>,
    index: BTreeMap<String,String>,
    write_name: Arc<AtomicU64>,
    // 压缩触发统计
    compress_counter: Arc<AtomicUsize>,
    // 数据文件写缓冲区
    write_buf: Arc<Mutex<ByteVec>>,
    // WAL 日志文件写句柄
    wal_writer: Arc<Mutex<BufWriter<File>>>,
    wal_write_name: Arc<AtomicU64>,
}
impl LogEngine {

    /// 从指定的数据目录打开一个 LogEngine
    pub fn open() -> Result<Self> {

        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        fs::create_dir_all(&data_dir)?;

        // 初始化 wal_writer
        let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
        fs::create_dir_all(&log_dir)?;
        let log_names = sorted_gen_list(log_dir.as_path(),
                                         SERVER_CONFIG.log_file_extension.as_ref(),
                                         SERVER_CONFIG.log_file_suffix.as_ref())?;
        let wal_writer;
        let wal_write_name;
        if log_names.is_empty() {
            wal_write_name = AtomicU64::new(0);
            wal_writer = BufWriter::new(
                open_option(get_log_path(log_dir.as_path(),
                                         wal_write_name.load(Ordering::SeqCst),
                                         SERVER_CONFIG.log_file_suffix.as_str()))?
            );

        }else {
            wal_write_name = AtomicU64::new(*log_names.last().unwrap());
            let last_log_file = open_option(get_log_path(log_dir.as_path(),
                                                         wal_write_name.load(Ordering::SeqCst),
                                                         SERVER_CONFIG.log_file_suffix.as_str()))?;
            if last_log_file.metadata()?.len() >= SERVER_CONFIG.log_file_max_size as u64 {
                wal_write_name.fetch_add(1,Ordering::SeqCst);
                wal_writer = BufWriter::new(
                    open_option(get_log_path(log_dir.as_path(),
                                             wal_write_name.load(Ordering::SeqCst),
                                             SERVER_CONFIG.log_file_suffix.as_str()))?
                );

            }else {
                wal_writer = BufWriter::new(last_log_file);
            }
        }

        // 初始化 其它
        let readers = Arc::new(Mutex::new(HashMap::<u64,BufReader<File>>::new()));
        let mut index = BTreeMap::<String,String>::new();
        let compress_counter = Arc::new(AtomicUsize::new(0_usize));

        let data_names = sorted_gen_list(data_dir.as_path(),
                                         SERVER_CONFIG.data_file_extension.as_ref(),
                                         SERVER_CONFIG.data_file_suffix.as_ref())?;
        log::info!("load data files:{:?}",&data_names);

        for &name in &data_names {
            let mut reader = BufReader::new(
                File::open(get_log_path(data_dir.as_path(), name, SERVER_CONFIG.data_file_suffix.as_str()))?
            );
            // 加载log文件到index中，在这个过程中不断执行insert 和remove操作，根据set 或者 rm
            // 同时记录压缩统计
            compress_counter.fetch_add(load(&mut reader, &mut index)?,Ordering::SeqCst);
            // 一个log 文件对应一个  bufreader
            readers.lock().unwrap().insert(name, reader);
        }
        let curr_log = data_names.last();

        // writer 初始化
        let (writer,write_name) = match curr_log {
            // 如果存在最后写入数据的文件
            Some(&name) => {
                let file = open_option(get_log_path(&data_dir,name,SERVER_CONFIG.data_file_suffix.as_str()))?;
                if file.metadata()?.len() >= SERVER_CONFIG.file_max_size {
                    // 如果文件大小超过规定大小，则创建新文件
                    (
                        Arc::new(Mutex::new(
                            BufWriter::new(open_option(get_log_path(&data_dir,
                                                                    name + 1,
                                                                    SERVER_CONFIG.data_file_suffix.as_str()))?
                            )
                        )),
                        name + 1
                    )
                }else {
                    (
                        Arc::new(Mutex::new(
                            BufWriter::new(file)
                        )),
                        name
                    )
                }
            },
            None => {
                // 如果不存在任何一个文件，则创建初始的文件
                let first_file = open_option(get_log_path(&data_dir,0,SERVER_CONFIG.data_file_suffix.as_str()))?;
                readers.lock().unwrap().insert(0,BufReader::new(first_file.try_clone()?));
                (
                    Arc::new(Mutex::new(
                        BufWriter::new(first_file)
                    )),
                    0
                )
            }
        };

        let write_name = Arc::new(AtomicU64::new(write_name));
        // 开启压缩线程
        compress(readers.clone(),
                 write_name.clone(),
                 compress_counter.clone())?;

        info!("compress_counter ====> -| {} |-",compress_counter.load(Ordering::SeqCst));
        Ok(LogEngine {
            readers, writer,
            index, write_name,
            compress_counter,
            write_buf: Arc::new(Mutex::new(ByteVec::new())),
            wal_writer: Arc::new(Mutex::new(wal_writer)),
            wal_write_name: Arc::new(wal_write_name)
        })
    }

    /// 执行check_point。
    pub fn check_point(&mut self) -> Result<()> {
        let writer = self.writer.clone();
        let write_buf = self.write_buf.clone();
        let wal_write_name = self.wal_write_name.clone();
        let wal_writer = self.wal_writer.clone();

        thread::Builder::new()
            .name("check_point_thread".to_string())
            .spawn(move || -> Result<()>{
                loop {
                    sleep(Duration::from_secs(SERVER_CONFIG.compress_interval));
                    {
                        // 首先要flush write_buf
                        let mut _write_buf = write_buf.lock().unwrap();
                        flush(writer.clone(),_write_buf.deref_mut())?;
                    }
                    // 替换新的wal_writer 句柄
                    let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
                    let delete_max_name = wal_write_name.fetch_add(1,Ordering::SeqCst);
                    {
                        let mut new_wal_writer = wal_writer.lock().unwrap();
                        *new_wal_writer = BufWriter::new(
                            open_option(get_log_path(log_dir.as_path(),
                                                     wal_write_name.load(Ordering::SeqCst),
                                                     SERVER_CONFIG.log_file_suffix.as_str()))?);
                    }
                    // 删除旧的文件
                    let names = sorted_gen_list(log_dir.as_path(),
                                    SERVER_CONFIG.log_file_extension.as_str(),
                                    SERVER_CONFIG.log_file_suffix.as_str())?;
                    let names: Vec<&u64> = names.iter().filter(|&&ele| {
                        ele <= delete_max_name
                    }).collect();

                    for name in names {
                        remove_file(get_log_path(&log_dir,
                                                 *name,
                                                 SERVER_CONFIG.log_file_suffix.as_str())
                            .as_path())?;
                    }
                };
            })?;
        Ok(())
    }

    /// 重演 WAL 日志文件，如果有必要的话,只在服务启动时调用一次
    pub fn try_recovery(&self) {
        // todo

    }
}
impl KvsEngine for LogEngine {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // command_type key存在执行set 必定是 update；反之亦然
        let command_type;
        if self.index.get(key).is_some() {
            command_type = CommandType::Update;
            self.compress_counter.fetch_add(1_usize,Ordering::SeqCst);
        }else {
            command_type = CommandType::Insert;
        }
        // 写 WAL 的逻辑先于其他逻辑，这里失败将不会走下面的逻辑
        if let Err(err) = write_ahead_log(self.wal_writer.clone(),
                                          &self.wal_write_name,
                                          &command_type,
                                          key,
                                          Some(value)) {
            self.compress_counter.fetch_sub(1_usize,Ordering::SeqCst);
            return Err(err);
        }

        // 放入内存中
        self.index.insert(key.to_string(),value.to_string());
        let kv = KVPair::new(key.to_string(),Some(value.to_string()));
        // 持久化data文件
        data_log_append(
            DataAppend {
                write_buf: self.write_buf.clone(),
                writer: self.writer.clone(),
                readers: self.readers.clone(),
                command_type,
                write_name: self.write_name.clone(),
                kv,
            }
        )?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        // 从内存中获取
        Ok(self.index.get(key).cloned())
    }

    #[allow(unused_variables)]
    fn scan(&self, range: Scans) -> Result<Option<Vec<String>>> {
        todo!()
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        write_ahead_log(self.wal_writer.clone(),
                        &self.wal_write_name,
                        &CommandType::Delete,
                        key,
                        None)?;
        // 内存值移除
        let opt = self.index.remove(key);
        match opt {
            Some(_) => {
                self.compress_counter.fetch_add(1_usize,Ordering::SeqCst);
                let kv = KVPair::new(key.to_string(),None);
                // 持久化data 文件
                data_log_append(
                    DataAppend {
                        write_buf: self.write_buf.clone(),
                        writer: self.writer.clone(),
                        readers: self.readers.clone(),
                        command_type: CommandType::Delete,
                        write_name: self.write_name.clone(),
                        kv,
                    }
                )?;
                Ok(())
            },
            None => {
                Err(anyhow::Error::from(WiscError::KeyNotExist(key.to_string())))
            }
        }

    }
}


/// 默认的文件句柄option
pub fn open_option(path: PathBuf) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .create(true)
        .write(true)
        .append(true)
        .open(path)?)
}

///  排序数据目录下的所有的数据文件，获取文件名集合
fn sorted_gen_list(path: &Path,file_extension: &str, file_suffix:&str) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> anyhow::Result<_> {
            Ok(res?.path())
        })
        // 过滤属于文件的path，并且文件扩展名是 wisc
        .filter(|path| path.is_file() && path.extension() == Some(OsStr::new(file_extension)))
        // 过滤出需要的 path
        .flat_map(|path| {
            // 获取文件名
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(file_suffix))
                .map(str::parse::<u64>)
        })
        // 扁平化，相当于去除flat 包装，取得里面的 u64 集合
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// 根据数据目录和文件编号获取指定的文件地址
pub fn get_log_path(dir: &Path, gen: u64, file_suffix: &str) -> PathBuf {
    dir.join(format!("{}{}", gen, file_suffix))
}

/// 加载单个文件中的单个record
///
/// (KVPair,bool)  bool 代表是否需要压缩
pub fn process_record<R: Read >(reader: &mut R) -> Result<(KVPair,bool)> {
    let mut header_buf = ByteVec::with_capacity(RECORD_HEADER_SIZE);
    {
        reader.by_ref().take(RECORD_HEADER_SIZE as u64).read_to_end(&mut header_buf)?;
    }
    // 得到record header
    let header = bincode::deserialize::<RecordHeader>(header_buf.as_slice())?;

    info!("load header:{:?}",&header);
    let saved_checksum = header.checksum;

    let data_len = header.data_len;
    // data 字节数据
    let mut data_buf = ByteVec::with_capacity(data_len as usize);
    {
        reader.by_ref().take(data_len as u64).read_to_end(&mut data_buf)?;
    }

    let checksum = checksum(data_buf.as_slice());
    if checksum != saved_checksum {
        // 数据损坏
        return Err(anyhow::Error::from(
            WiscError::DataCorruption { checksum, saved_checksum }
        ))
    }
    let kv = bincode::deserialize::<KVPair>(data_buf.as_slice())?;
    info!("load data:{:?}",&kv);
    Ok((kv, header.command_type != CommandType::Insert as u8))
}


/// 循环加载单个文件中的record
pub fn load(
    reader: &mut BufReader<File>,
    index: &mut BTreeMap<String,String>,
) -> Result<usize> {
    let mut compress_counter = 0_usize;
    loop {
        let recorde = process_record(reader);
        let (kv,is_compress) = match recorde {
            Ok(result) => result,
            Err(err) => {
                if is_eof_err(&err) {
                    break;
                }else { return Err(err); }
            }
        };
        match kv.value {
            Some(value) => {
                // 将数据放入内存
                index.insert(kv.key, value);
            },
            None =>{
                // 从内存中移除
                // 文件中的删除会根据 command_type 去进行
                index.remove(&kv.key);
            }
        }
        // 统计 compress_counter
        if is_compress {compress_counter += 1 }
    }
    Ok(compress_counter)
}


/// 写 WAL 日志。
///
/// 就传统的关系型数据库，通常来说我们会在一次事务提交之后对`log_buf`中的数据进行 flush。
/// 当此次操作的 WAL 日志持久化之后我们才返回客户端此次操作success。
///
/// 另外的可选方案是定时flush log_buf，例如每秒flush一次，极端情况下，我们可能丢失一秒内
/// 客户端的操作数据。
///
/// 如果我们需要数据库的基本ACID特性，我们将不会选择定时，而是选择 用户提交事务即持久化。
/// 用户可能一次进行单条数据修改，也可能多个。
///
/// 因此现在我们选择：每次客户端的数据修改操作（除了 查询操作）都进行 log 记录并 flush。
///
///PS：这里可能显得有些多余，因为我们可以选择直接在data_file存储中进行每次flush，因为目前来说
/// 我们的data_file 也是append 写入的。但是别忘了，我们将要在不就的将来实现 LSM 模型存储，在
/// LSM 模型的data_file（SSTable） 中,数据将不会按照用户的写入顺序单条append 写入，因此WAL
/// 的存在必不可少。
pub fn write_ahead_log(wal_writer: Arc<Mutex<BufWriter<File>>>,
                       wal_write_name: &AtomicU64,
                       command_type: &CommandType,
                       key: &str,
                       value: Option<&str>) -> Result<()> {
    let log_str = match command_type {
        CommandType::Insert => {
            format!("{} {} {};","insert",key,value.unwrap())
        },
        CommandType::Delete =>{
            format!("{} {};","delete",key)
        },
        CommandType::Update => {
            format!("{} {} {};","update",key,value.unwrap())
        }
    };

    let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
    let last_log_file = open_option(get_log_path(log_dir.as_path(),
                                                 wal_write_name.load(Ordering::SeqCst),
                                                 SERVER_CONFIG.log_file_suffix.as_str()))?;
    {
        let mut _wal_writer = wal_writer.lock().unwrap();
        if last_log_file.metadata()?.len() >= SERVER_CONFIG.log_file_max_size as u64 {
            wal_write_name.fetch_add(1,Ordering::SeqCst);
            *_wal_writer = BufWriter::new(
                open_option(get_log_path(log_dir.as_path(),
                                         wal_write_name.load(Ordering::SeqCst),
                                         SERVER_CONFIG.log_file_suffix.as_str()))?
            );

        }else {
            *_wal_writer = BufWriter::new(last_log_file);
        }

        _wal_writer.write_all(log_str.as_bytes())?;
        _wal_writer.flush()?;
    }

    Ok(())
}

#[cfg(test)]
mod test {

    #[test]
    fn test() {


    }
}