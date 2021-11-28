//! 日志存储实现 引擎核心

use std::fs::{File, OpenOptions, remove_file};
use std::collections::btree_map::BTreeMap;
use std::{env, fs, thread};
use std::io::{BufReader, Read, BufWriter};
use std::collections::HashMap;

use crate::{KvsEngine};
use crate::engines::base_log_engine::record::{RECORD_HEADER_SIZE, RecordHeader, KVPair, CommandType};
use crate::engines::{Scans, write_ahead_log, init_wal};
use crate::common::fn_util::{checksum, is_eof_err, open_option_default, sorted_gen_list, get_file_path};
use crate::common::types::ByteVec;
use crate::common::error_enum::WiscError;
use crate::config::SERVER_CONFIG;

use anyhow::Result;
use log::{info,warn};
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use crate::engines::base_log_engine::compress::compress;
use std::sync::{Arc, Mutex};
use crate::engines::base_log_engine::persistence::{data_log_append, DataAppend, flush};
use std::ops::DerefMut;
use std::thread::sleep;
use std::time::Duration;
use crate::client::command_parser;
use crate::server::client_command_process;

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
        let (wal_writer,wal_write_name) = init_wal()?;

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
                File::open(get_file_path(data_dir.as_path(), name, SERVER_CONFIG.data_file_suffix.as_str()))?
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
                let file = open_option_default(get_file_path(&data_dir, name, SERVER_CONFIG.data_file_suffix.as_str()))?;
                if file.metadata()?.len() >= SERVER_CONFIG.file_max_size {
                    // 如果文件大小超过规定大小，则创建新文件
                    (
                        Arc::new(Mutex::new(
                            BufWriter::new(open_option_default(get_file_path(&data_dir,
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
                let first_file = open_option_default(get_file_path(&data_dir, 0, SERVER_CONFIG.data_file_suffix.as_str()))?;
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
                    info!("[开始执行 check_point]");

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
                            open_option_default(get_file_path(log_dir.as_path(),
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
                        remove_file(get_file_path(&log_dir,
                                                  *name,
                                                  SERVER_CONFIG.log_file_suffix.as_str())
                            .as_path())?;
                    }

                    info!("[执行结束 check_point]");
                };

            })?;
        Ok(())
    }

    /// 重演 WAL 日志文件。
    /// 注意：只在服务启动时调用一次
    ///
    /// 这产生的效果将会按照check_point最后的点重新执行客户端的命令，覆盖之后的所有数据
    /// 恢复到宕机前的数据
    pub fn try_recovery(&mut self) -> Result<()> {
        info!("[尝试重演 xlog 开始...]");
        let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
        let names = sorted_gen_list(log_dir.as_path(),
                                    SERVER_CONFIG.log_file_extension.as_str(),
                                    SERVER_CONFIG.log_file_suffix.as_str())?;

        // 按照文件名顺序读取log 目录中所有的 .xlog 文件,
        for name in names {
            let file = OpenOptions::new().read(true)
                .open(get_file_path(
                    &log_dir,
                    name,
                    SERVER_CONFIG.log_file_suffix.as_str())
                )?;

            let mut xlog_reader = BufReader::new(file);
            let mut command_str = String::new();
            if command_str.is_empty() {
                continue;
            }
            xlog_reader.read_to_string(&mut command_str)?;
            let command_vec: Vec<String> = command_str.split(';').map(|ele| {
                format!("{};",ele)
            }).collect();
            // 调用不同命令对应的方法，重演所有的 command 串。
            command_vec.iter().for_each(|ele| {
                match command_parser(ele.as_str()) {
                    Some(_command) => {
                        client_command_process(&_command,self);
                    },
                    None => {warn!("[故障恢复命令解析故障：{}]",ele)},
                };
            });

        }

        info!("[尝试重演 xlog 完成。]");
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
