//! log_record 数据模型
use serde_derive::{Serialize,Deserialize};
use anyhow::Result;
use crate::common::fn_util::{checksum, open_option_default};
use std::io::{BufWriter, Write};
use std::fs::{File, create_dir_all};
use log::info;
use crate::common::types::ByteVec;
use std::collections::HashMap;
use std::env;
use crate::config::SERVER_CONFIG;
use chrono::Local;

/// record header的大小，固定值
pub const RECORD_HEADER_SIZE:usize = 9;
/// block 大小：32KB
pub const BLOCK_SIZE:usize = 1024 * 32;
/// checksum (4 bytes), data_length (4 bytes), type (1 byte).
pub const HEADER_SIZE:usize = 4 + 4 + 1;
/// 日志文件达到预定大小（4MB）
pub const LOG_FILE_MAX_SIZE:usize = 1024 * 1024 * 4;

/// 存储磁盘的完整实体
#[derive(Debug)]
pub struct LogRecord {
    block_writer: BufWriter<File>,
    last_record_type: RecordType,
    data_rest_len: usize,
}
impl LogRecord {
    // 初始化 LogRecord 实体
    pub fn new() -> Result<Self> {
        let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
        create_dir_all(log_dir.clone())?;
        
        let file_name = format!("{}.{}",Local::now().timestamp_millis(),SERVER_CONFIG.log_file_extension);
        let log_file = open_option_default(log_dir.join(file_name.as_str()))?;
        let block_writer = BufWriter::with_capacity(BLOCK_SIZE,log_file);
        Ok(LogRecord {
            block_writer,
            last_record_type: RecordType::NoneType,
            data_rest_len: 0
        })
    }

    /// 往 log 中添加 record
    pub fn add_records(&mut self,data: &KVPair) -> Result<()> {
        let mut data_byte = bincode::serialize(data)?;
        self.add(&mut data_byte)?;
        Ok(())
    }

    fn add(&mut self, data_byte: &mut ByteVec) -> Result<()> {
        // 结束递归条件
        if data_byte.len() == 0 {
            return Ok(());
        }

        // 当前block剩余的空间
        let block_free_size =  self.block_writer.capacity() - self.block_writer.buffer().len();

        // 可以存放record，处理KVPair
        if block_free_size > HEADER_SIZE {
            // 去掉头长度的空间
            let data_free_size = block_free_size - HEADER_SIZE;

            // 如果当前的数据在当前的block中放不下
            if data_byte.len() > data_free_size {
                // 根据data_free_size 切割
                let mut rest_data_byte = data_byte.split_off(data_free_size);
                match self.last_record_type {
                    RecordType::NoneType => {
                        self.write_for_type(data_byte, RecordType::FirstType)?;
                    },
                    RecordType::FirstType | RecordType::MiddleType => {
                        self.write_for_type(data_byte, RecordType::MiddleType)?;
                    },
                    _ => {}
                }
                self.block_writer.flush()?;
                // 递归调用
                self.add(&mut rest_data_byte);
            }
            // 如果当前的数据在当前的block中能放下且大于 header 长度
            else {
                match self.last_record_type {
                    RecordType::NoneType => {
                        self.write_for_type(data_byte, RecordType::FullType)?;
                    },
                    RecordType::FirstType | RecordType::MiddleType => {
                        self.write_for_type(data_byte, RecordType::LastType)?;
                    },
                    _ => {}
                }
            }
        }
        // 下面两种情况属于特例
        else if block_free_size == HEADER_SIZE {
            // 存放一个 数据长度为0的 header
            let head_bytes = bincode::serialize(&RecordHeader::default())?;
            self.block_writer.write_all(head_bytes.as_slice())?;
        }
        else {
            // 使用 [0_u8;block_free_size] 填充
            self.block_writer.write_all(&vec![0_u8;block_free_size].as_slice())?;
        }

        // 满 BLOCK_SIZE 异步flush（这里相对于fsync 调用来说）
        if self.block_writer.buffer().len() >= BLOCK_SIZE {
            self.block_writer.flush()?;
            info!("block_writer flush, curr_len:{}",self.block_writer.buffer().len());

        }

        Ok(())

    }

    fn write_for_type(&mut self,
                      data_byte: &mut ByteVec,
                      _type: RecordType) -> Result<()>
    {
        let checksum = checksum(data_byte.as_slice());
        let record_header = RecordHeader::new(checksum,
                                              data_byte.len() as u32,
                                              RecordType::FullType as u8);

        let mut header_byte = bincode::serialize(&record_header)?;
        header_byte.append(data_byte);
        self.block_writer.write_all(header_byte.as_slice())?;
        Ok(())
    }
}

/// header布局
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RecordHeader {
    pub checksum: u32,
    pub data_len: u32,
    pub _type: u8,
}
impl RecordHeader {
    pub fn new(checksum: u32, data_len: u32, _type: u8) -> Self {
        RecordHeader { checksum, data_len, _type }
    }
}
impl Default for RecordHeader {
    fn default() -> Self {
        RecordHeader {
            checksum: 0,
            data_len: 0,
            _type: RecordType::NoneType as u8
        }
    }
}


/// record_type
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum RecordType {
    // 空数据header,或者默认的 type
    NoneType,

    FullType,
    // 分段的类型
    FirstType,
    MiddleType,
    LastType,
}

/// 操作类型 可取：`Insert` `Update` `Delete`
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum CommandType {
    Insert,
    Update,
    Delete
}

/// record 键值对
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct KVPair {
    pub key: String,
    // some 和 none 将占一个字节，如果value的大小是8，实际存储将占9
    pub value: Option<String>,
}
impl KVPair {
    pub fn new(key: String, value: Option<String>) -> Self {
        KVPair { key, value }
    }
}

#[cfg(test)]
mod test {
    use super::*;


    #[test]
    fn test() {
    }

}