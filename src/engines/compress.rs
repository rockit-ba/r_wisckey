//! 压缩处理逻辑

use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use std::{thread, env};

use anyhow::Result;
use log::info;
use crate::config::SERVER_CONFIG;
use std::thread::sleep;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::io::{BufWriter, BufReader, Write, SeekFrom, Seek};
use std::fs::{File, remove_file};
use crate::engines::base_log::{open_option, get_log_path, load};
use std::collections::{HashMap, BTreeMap};
use crate::engines::record::{CommandType, KVPair, RecordHeader};
use crate::common::fn_util::checksum;
use std::ops::{ DerefMut};

/// 压缩任务的线程 `name`
const COMPRESS_THREAD:&str = "compress_thread";

/// 定时检查 compress_counter 并执行压缩
///
/// 执行完一次压缩之后
pub fn compress(readers: Arc<Mutex<HashMap<u64,BufReader<File>>>>,
                write_name: Arc<AtomicU64>,
                compress_counter: Arc<AtomicUsize>) -> Result<()>
{
    thread::Builder::new()
        .name(COMPRESS_THREAD.to_string())
        .spawn(move || -> Result<()>{
            let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
            loop {
                // 如果达到压缩阈值，开始压缩
                if compress_counter.load(Ordering::SeqCst) >= SERVER_CONFIG.compress_threshold {
                    info!("===========> Compress....");
                    // todo 压缩之前先触发check_point，然后更换xlog 文件句柄，清空之前的wal 日志。
                    // 触发check_point 的目的是为了清除 wal 日志，
                    {
                        let mut readers =  readers.lock().unwrap();
                        // 将所有文件中的数据加载到内存中，
                        let mut index = BTreeMap::<String,String>::new();
                        // 保留压缩后要移除的文件名
                        let mut file_names = Vec::<u64>::new();
                        for (file_name,reader) in readers.iter_mut() {
                            reader.seek(SeekFrom::Start(0))?;
                            // 同时更新 compress_counter
                            compress_counter.fetch_sub(load(reader, &mut index)?,Ordering::SeqCst);
                            file_names.push(*file_name);
                        }
                        file_names.iter().for_each(|gen| {
                            readers.remove(gen);
                        });
                        info!("Will compress file_names:{:?}",&file_names);

                        // 生成要写入压缩数据的文件编号
                        write_name.fetch_add(1_u64,Ordering::SeqCst);
                        let gen = write_name.load(Ordering::SeqCst);
                        // 创建对应的存放新数据的 file 句柄
                        let mut new_writer = BufWriter::new(open_option(get_log_path(&data_dir,gen,SERVER_CONFIG.data_file_suffix.as_str()))?);
                        //遍历内存数据将数据写入磁盘
                        for (key,value) in index.iter() {
                            let kv = KVPair::new(key.to_string(),Some(value.to_string()));
                            // 持久化log 文件
                            append( readers.deref_mut(),
                                    &write_name,
                                    &mut new_writer,
                                    // 压缩之后的类型都是insert的
                                    CommandType::Insert,
                                    &kv)?;
                        }
                        for file_name in file_names.iter() {
                            remove_file(get_log_path(&data_dir,
                                                     *file_name,
                                                     SERVER_CONFIG.data_file_suffix.as_str())
                                .as_path())?;
                        }
                    }
                    info!("===========> Compress end ...");
                };
                sleep(Duration::from_secs(SERVER_CONFIG.compress_interval));
            }
        })?;

    Ok(())
}

fn append(readers: &mut HashMap<u64,BufReader<File>>,
          write_name: &Arc<AtomicU64>,
          writer: &mut BufWriter<File>,
          command_type: CommandType,
          kv: &KVPair) -> Result<()>
{
    if writer.get_ref().metadata()?.len() >= SERVER_CONFIG.file_max_size {
        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        // 如果文件大小超过规定大小，则创建新文件
        write_name.fetch_add(1_u64,Ordering::SeqCst);
        let gen = write_name.load(Ordering::SeqCst);

        let new_file = open_option(get_log_path(&data_dir,gen,SERVER_CONFIG.data_file_suffix.as_str()))?;
        *writer = BufWriter::new(new_file.try_clone()?);
        readers.insert(gen,BufReader::new(new_file));
        info!("Create new data file :{}",gen);
    }

    let mut data_byte = bincode::serialize(kv)?;
    let header = RecordHeader::new(command_type as u8,
                                   checksum(data_byte.as_slice()),
                                   data_byte.len() as u32);

    let mut header_byte = bincode::serialize(&header)?;

    header_byte.append(&mut data_byte);
    writer.write_all(header_byte.as_slice())?;
    writer.flush()?;
    Ok(())
}