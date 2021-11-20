//! 日志存储实现 引擎核心

use std::fs::{File, OpenOptions};
use std::collections::btree_map::BTreeMap;
use std::{env, fs};
use std::path::{PathBuf, Path};
use std::ffi::OsStr;
use std::io::{BufReader, Read, BufWriter, Write,};
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

/// 存储引擎
#[derive(Debug)]
pub struct LogEngine {
    readers: Arc<Mutex<HashMap<u64,BufReader<File>>>>,
    writer: BufWriter<File>,
    index: BTreeMap<String,String>,
    write_name: Arc<AtomicU64>,
    // 压缩触发统计
    compress_counter: Arc<AtomicUsize>,
}
impl LogEngine {

    /// 从指定的数据目录打开一个 LogEngine
    pub fn open() -> anyhow::Result<Self> {

        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        fs::create_dir_all(&data_dir)?;

        let readers = Arc::new(Mutex::new(HashMap::<u64,BufReader<File>>::new()));
        let mut index = BTreeMap::<String,String>::new();
        let compress_counter = Arc::new(AtomicUsize::new(0_usize));

        let log_names = sorted_gen_list(data_dir.as_path())?;
        log::info!("load data files:{:?}",&log_names);

        for &name in &log_names {
            let mut reader = BufReader::new(
                File::open(get_log_path(data_dir.as_path(), name))?
            );
            // 加载log文件到index中，在这个过程中不断执行insert 和remove操作，根据set 或者 rm
            // 同时记录压缩统计
            compress_counter.fetch_add(load(&mut reader, &mut index)?,Ordering::SeqCst);
            // 一个log 文件对应一个  bufreader
            readers.lock().unwrap().insert(name, reader);
        }
        let curr_log = log_names.last();

        // writer 初始化
        let (writer,write_name) = match curr_log {
            // 如果存在最后写入数据的文件
            Some(&name) => {
                let file = open_option(get_log_path(&data_dir,name))?;
                if file.metadata()?.len() >= SERVER_CONFIG.file_max_size {
                    // 如果文件大小超过规定大小，则创建新文件
                    (
                        BufWriter::new(open_option(get_log_path(&data_dir,name + 1))?),
                        name + 1
                    )
                }else {
                    (
                        BufWriter::new(file),
                        name
                    )
                }
            },
            None => {
                // 如果不存在任何一个文件，则创建初始的文件
                let first_file = open_option(get_log_path(&data_dir,0))?;
                readers.lock().unwrap().insert(0,BufReader::new(first_file.try_clone()?));
                (
                    BufWriter::new(first_file),
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
            readers,
            writer,
            index,
            write_name,
            compress_counter
        })
    }

    /// 往文件中添加 操作数据
    fn append(&mut self, command_type: CommandType, kv: &KVPair) -> Result<()> {
        if self.writer.get_ref().metadata()?.len() >= SERVER_CONFIG.file_max_size {
            let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
            // 如果文件大小超过规定大小，则创建新文件
            self.write_name.fetch_add(1_u64,Ordering::SeqCst);
            let gen = self.write_name.load(Ordering::SeqCst);

            let new_file = open_option(get_log_path(&data_dir,gen))?;

            self.writer =
                BufWriter::new(new_file.try_clone()?);

            self.readers.lock().unwrap().insert(gen,BufReader::new(new_file));
            info!("create new data file :{}",gen);
        }

        let mut data_byte = bincode::serialize(kv)?;
        let header = RecordHeader::new(command_type as u8,
                                       checksum(data_byte.as_slice()),
                                       data_byte.len() as u32);

        let mut header_byte = bincode::serialize(&header)?;
        info!("append header:{:?}",&header);
        info!("append data:{:?}",kv);
        /*
         将header_byte 和 data_byte进行合并，并且write，注意顺序不能替换
         因为我们首先是读取定长的 header
         */
        header_byte.append(&mut data_byte);
        self.writer.write_all(header_byte.as_slice())?;
        self.writer.flush()?;
        Ok(())
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

        // 放入内存中
        self.index.insert(key.to_string(),value.to_string());
        let kv = KVPair::new(key.to_string(),Some(value.to_string()));
        // 持久化log 文件
        self.append(command_type,&kv)?;
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
        // 内存值移除
        let opt = self.index.remove(key);
        match opt {
            Some(_) => {
                self.compress_counter.fetch_add(1_usize,Ordering::SeqCst);
                let kv = KVPair::new(key.to_string(),None);
                // 持久化log 文件
                self.append(CommandType::Delete,&kv)?;
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
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> anyhow::Result<_> {
            Ok(res?.path())
        })
        // 过滤属于文件的path，并且文件扩展名是 wisc
        .filter(|path| path.is_file() && path.extension() == Some(SERVER_CONFIG.data_file_extension.as_ref()))
        // 过滤出需要的 path
        .flat_map(|path| {
            // 获取文件名
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(&SERVER_CONFIG.data_file_suffix))
                .map(str::parse::<u64>)
        })
        // 扁平化，相当于去除flat 包装，取得里面的 u64 集合
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// 根据数据目录和文件编号获取指定的文件地址
pub fn get_log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}{}", gen, &SERVER_CONFIG.data_file_suffix))
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

