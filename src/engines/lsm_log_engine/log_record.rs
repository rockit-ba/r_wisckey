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
use std::fmt::{Display, Formatter};

/// block 大小：32KB
pub const BLOCK_SIZE:usize = 1024 * 32;
/// checksum (4 bytes), data_length (4 bytes), type (1 byte).
pub const RECORD_HEADER_SIZE:usize = 4 + 4 + 1;
/// 日志文件达到预定大小（4MB）
pub const LOG_FILE_MAX_SIZE:usize = 1024 * 1024 * 4;

/// 存储磁盘的引用结构
#[derive(Debug)]
pub struct LogRecord {
    // 当前 log 文件的写句柄
    block_writer: BufWriter<File>,
    // 上一次add_process的RecordType
    last_record_type: RecordType,
    // 当前data 剩余长度
    data_rest_len: usize,
    // 当前block剩余的空间，初始化就是满的 BLOCK_SIZE
    block_writer_rest_len: usize,
}
impl LogRecord {
    /// 初始化 LogRecord 实体
    pub fn new() -> Result<Self> {
        let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
        create_dir_all(log_dir.clone())?;
        
        let file_name = format!("{}.{}", Local::now().timestamp_millis(), SERVER_CONFIG.log_file_extension);
        let log_file = open_option_default(log_dir.join(file_name.as_str()))?;
        // 当前 log 文件的写句柄
        let block_writer = BufWriter::with_capacity(BLOCK_SIZE,log_file);
        Ok(LogRecord {
            block_writer,
            last_record_type: RecordType::NoneType,
            data_rest_len: 0,   // 无data
            block_writer_rest_len: BLOCK_SIZE,
        })
    }

    /// 往 log 中添加 record
    pub fn add_records(&mut self,data: &KVPair) -> Result<()> {
        let mut data_byte = bincode::serialize(data)?;
        self.add_process(&mut data_byte)?;
        Ok(())
    }
    /// 单独的处理流程。分离方便递归调用
    fn add_process(&mut self, data_byte: &mut ByteVec) -> Result<()> {
        // 结束递归条件
        if data_byte.len() == 0 {
            return Ok(());
        }

        // 可以存放（部分或者全部） record，处理KVPair
        if self.block_writer_rest_len > RECORD_HEADER_SIZE {
            // 去掉头长度的空间
            let data_free_size = self.block_writer_rest_len - RECORD_HEADER_SIZE;

            // 如果当前的数据在当前的block中放不下
            if data_byte.len() > data_free_size {
                // 根据data_free_size 切割
                let mut rest_data_byte = data_byte.split_off(data_free_size);
                match self.last_record_type {
                    RecordType::NoneType | RecordType::FullType | RecordType::LastType => {
                        self.write_for_type(data_byte, RecordType::FirstType)?;
                    },
                    RecordType::FirstType | RecordType::MiddleType => {
                        self.write_for_type(data_byte, RecordType::MiddleType)?;
                    },
                }
                // 递归调用
                self.add_process(&mut rest_data_byte);
            }
            // 如果当前的数据在当前的block中能放下且大于 header 长度
            else {
                match self.last_record_type {
                    RecordType::NoneType | RecordType::FullType | RecordType::LastType => {
                        self.write_for_type(data_byte, RecordType::FullType)?;
                    },
                    RecordType::FirstType | RecordType::MiddleType => {
                        self.write_for_type(data_byte, RecordType::LastType)?;
                    }
                }
            }
        }

        // 下面两种都属于不可存放 record 的情况
        else if self.block_writer_rest_len == RECORD_HEADER_SIZE {
            // 存放一个 数据长度为0的 header
            let head_bytes = bincode::serialize(&RecordHeader::default())?;
            self.block_writer.write_all(head_bytes.as_slice())?;
            self.block_writer.flush()?;
            self.block_writer_rest_len = BLOCK_SIZE;
        }
        else {
            // 使用 [0_u8;block_free_size] 填充
            self.block_writer.write_all(&vec![0_u8;self.block_writer_rest_len].as_slice())?;
            self.block_writer.flush()?;
            self.block_writer_rest_len = BLOCK_SIZE;
        }
        // 用来处理不可存放 record 的情况，因为数据并未存入write，还需要接着调用
        if self.data_rest_len > 0 {
            self.add_process(data_byte);
        }

        Ok(())
    }

    /// 写 指定type的record；并flush 和更新 block_writer_rest_len
    fn write_for_type(&mut self,
                      data_byte: &mut ByteVec,
                      _type: RecordType) -> Result<()>
    {
        let checksum = checksum(data_byte.as_slice());
        let record_header = RecordHeader::new(checksum,
                                              data_byte.len() as u32,
                                              _type.clone() as u8);

        let mut header_byte = bincode::serialize(&record_header)?;
        header_byte.append(data_byte);

        self.block_writer.write_all(header_byte.as_slice())?;
        // 每写一条record就flush
        self.block_writer.flush()?;
        // 注意，不能直接重置为 BLOCK_SIZE，因为它可能是不满 block的
        self.block_writer_rest_len = self.block_writer_rest_len - data_byte.len();
        self.last_record_type = _type;
        info!("当前record type：{:?}",self.last_record_type);
        Ok(())
    }
}

/// header 结构布局
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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
    use std::io::Read;
    use crate::common::fn_util::log_init;

    #[test]
    fn add_records_01_test() -> Result<()> {
        log_init();
        let mut log_record = LogRecord::new()?;
        let mut str = String::new();
        File::open("a.txt")?.read_to_string(&mut str);
        let data = KVPair::new("a".to_string(),Some(str));
        log_record.add_records(&data)?;
        Ok(())
    }

    #[test]
    fn add_records_02_test() -> Result<()> {
        log_init();
        let mut log_record = LogRecord::new()?;
        let data1 = KVPair::new("a".to_string(),Some("aa".to_string()));
        log_record.add_records(&data1)?;

        let data2 = KVPair::new("b".to_string(),Some("bb".to_string()));
        log_record.add_records(&data2)?;
        let data3 = KVPair::new("c".to_string(),Some("cc".to_string()));
        log_record.add_records(&data3)?;
        Ok(())
    }

}