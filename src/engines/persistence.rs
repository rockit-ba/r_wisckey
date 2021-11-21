//! 数据持久化到磁盘

use std::sync::{Arc, Mutex};
use std::io::{BufWriter, BufReader, Write};
use std::fs::File;
use crate::engines::record::{CommandType, KVPair, RecordHeader};
use anyhow::Result;
use std::sync::atomic::{Ordering, AtomicU64};
use std::{env, thread};
use crate::engines::base_log::{open_option, get_log_path};
use std::collections::HashMap;
use crate::common::fn_util::checksum;
use crate::config::SERVER_CONFIG;
use log::info;
use crate::common::types::ByteVec;

const WRITE_BUF_FLUSH_THREAD:&str = "write_buf_flush_thread";

/// 数据持久化磁盘任务
pub fn data_log_append(write_buf: Arc<Mutex<ByteVec>>,
                       writer: Arc<Mutex<BufWriter<File>>>,
                       readers: Arc<Mutex<HashMap<u64,BufReader<File>>>>,
                       command_type: CommandType,
                       write_name: Arc<AtomicU64>,
                       kv: KVPair) -> Result<()> {
    thread::Builder::new()
        .name(WRITE_BUF_FLUSH_THREAD.to_string())
        .spawn(move || -> Result<()>{
            info!("{:?}",thread::current().name().unwrap());
            {
                let mut writer = writer.lock().unwrap();
                if writer.get_ref().metadata()?.len() >= SERVER_CONFIG.file_max_size {
                    let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
                    // 如果文件大小超过规定大小，则创建新文件
                    write_name.fetch_add(1_u64,Ordering::SeqCst);
                    let gen = write_name.load(Ordering::SeqCst);
                    let new_file = open_option(get_log_path(&data_dir,gen))?;

                    *writer = BufWriter::new(new_file.try_clone()?);
                    readers.lock().unwrap().insert(gen,BufReader::new(new_file));
                    info!("create new data file :{}",gen);
                }
            }
            // 处理 encode
            let mut data_byte = bincode::serialize(&kv)?;
            let header = RecordHeader::new(command_type as u8,
                                           checksum(data_byte.as_slice()),
                                           data_byte.len() as u32);

            let mut header_byte = bincode::serialize(&header)?;
            info!("append header:{:?}",&header);
            info!("append data:{:?}",&kv);
            /*
             将header_byte 和 data_byte进行合并，并且write，注意顺序不能替换
             因为我们首先是读取定长的 header
             */
            header_byte.append(&mut data_byte);
            info!("当前数据长度：{}",header_byte.len());
            {
                let mut write_buf = write_buf.lock().unwrap();
                write_buf.append(&mut header_byte);
                info!("write_buf数据长度：{}",write_buf.len());

                // 刷盘
                if write_buf.len() >= SERVER_CONFIG.write_buf_max_size {
                    let mut writer = writer.lock().unwrap();
                    loop {
                        // 如果当前缓冲区中数据长度小于 write_buf_max_size，则只writer，不flush。
                        if write_buf.len() < SERVER_CONFIG.write_buf_max_size {
                            writer.write_all(write_buf.as_slice())?;
                            // todo 添加了WAL 之后将会去除这里的 flush
                            writer.flush()?;
                            // 重置当前的 write_buf
                            *write_buf = ByteVec::new();
                            break;
                        }

                        let no_flush_len = writer.buffer().len();
                        // 分隔的长度为 writer 中为write_buf_max_size 减去 writer中未flush的数据长度，二者加起来的长度是 write_buf_max_size
                        let rest_buf = write_buf.split_off(SERVER_CONFIG.write_buf_max_size - no_flush_len);

                        writer.write_all(write_buf.as_slice())?;
                        info!("writer len:{}",writer.buffer().len());
                        writer.flush()?;
                        info!("writer len:{}",writer.buffer().len());

                        // 让当前的write_buf 等于剩下未flush的 data_buf
                        *write_buf = rest_buf;
                    }
                }
            }

            Ok(())
        })?;

    Ok(())
}
