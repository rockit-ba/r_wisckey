//! log_record 数据模型
use serde_derive::{Serialize,Deserialize};
use anyhow::Result;
use crate::common::fn_util::{checksum, open_option_default};
use std::io::{BufWriter, Write, BufReader, Read};
use std::fs::{File, create_dir_all, read_dir, OpenOptions};
use log::info;
use crate::common::types::ByteVec;
use std::collections::HashMap;
use std::env;
use crate::config::SERVER_CONFIG;
use chrono::Local;
use std::fmt::{Display, Formatter};
use std::ops::{DerefMut, Deref};
use std::borrow::{BorrowMut, Borrow};
use std::cmp::Ordering;

/// block 大小：32KB
pub const BLOCK_SIZE:usize = 1024 * 32;
/// checksum (4 bytes), data_length (4 bytes), type (1 byte).
pub const RECORD_HEADER_SIZE:usize = 4 + 4 + 1;
/// 日志文件达到预定大小（4MB）
pub const LOG_FILE_MAX_SIZE:usize = 1024 * 1024 * 4;

/// WAL日志写入的引用结构
#[derive(Debug)]
pub struct LogRecordWrite {
    // 当前 log 文件的写句柄
    block_writer: BufWriter<File>,
    // 上一次add_process的RecordType
    last_record_type: RecordType,
    // 当前data 剩余长度
    data_rest_len: usize,
    // 当前block剩余的空间，初始化就是满的 BLOCK_SIZE
    block_writer_rest_len: usize,
}
impl LogRecordWrite {
    /// 初始化 LogRecord 实体
    pub fn new() -> Result<Self> {
        let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
        create_dir_all(log_dir.clone())?;
        
        let file_name = format!("{}.{}", Local::now().timestamp_millis(), SERVER_CONFIG.log_file_extension);
        let log_file = open_option_default(log_dir.join(file_name.as_str()))?;
        // 当前 log 文件的写句柄
        let block_writer = BufWriter::with_capacity(BLOCK_SIZE,log_file);
        Ok(LogRecordWrite {
            block_writer,
            last_record_type: RecordType::None,
            data_rest_len: 0,   // 无data
            block_writer_rest_len: BLOCK_SIZE,
        })
    }

    /// 往 log 中添加 record
    ///
    /// 调用该方法之前初始化 Key，这里只负责写入
    pub fn add_records(&mut self,data: &Key, value: Option<&mut ByteVec>) -> Result<()> {
        let mut data_byte = bincode::serialize(data)?;
        if let Some(value) = value {
            data_byte.append(value);
        };
        self.add_process(&mut data_byte)?;
        Ok(())
    }
    /// 单独的处理流程。分离方便递归调用
    fn add_process(&mut self, data_byte: &mut ByteVec) -> Result<()> {
        // 结束递归条件
        if data_byte.is_empty() {
            return Ok(());
        }

        // 可以存放（部分或者全部） record
        match self.block_writer_rest_len.cmp(&RECORD_HEADER_SIZE) {
            Ordering::Greater => {
                // 去掉头长度的空间
                let data_free_size = self.block_writer_rest_len - RECORD_HEADER_SIZE;

                // 如果当前的数据在当前的block中放不下
                if data_byte.len() > data_free_size {
                    // 根据data_free_size 切割
                    let mut rest_data_byte = data_byte.split_off(data_free_size);
                    match self.last_record_type {
                        RecordType::None | RecordType::Full | RecordType::Last => {
                            self.write_for_type(data_byte, RecordType::First)?;
                        },
                        RecordType::First | RecordType::Middle => {
                            self.write_for_type(data_byte, RecordType::Middle)?;
                        },
                    }
                    // 递归调用
                    self.add_process(&mut rest_data_byte)?;
                }
                // 如果当前的数据在当前的block中能放下且大于 header 长度
                else {
                    match self.last_record_type {
                        RecordType::None | RecordType::Full | RecordType::Last => {
                            self.write_for_type(data_byte, RecordType::Full)?;
                        },
                        RecordType::First | RecordType::Middle => {
                            self.write_for_type(data_byte, RecordType::Last)?;
                        }
                    }
                }
            }
            // 下面两种都属于不可存放 record 的情况
            Ordering::Equal => {
                // 存放一个 数据长度为0的 header
                let head_bytes = bincode::serialize(&RecordHeader::default())?;
                self.block_writer.write_all(head_bytes.as_slice())?;
                self.block_writer.flush()?;
                self.block_writer_rest_len = BLOCK_SIZE;
            }
            Ordering::Less => {
                // 使用 [0_u8;block_free_size] 填充
                self.block_writer.write_all(vec![0_u8;self.block_writer_rest_len].as_slice())?;
                self.block_writer.flush()?;
                self.block_writer_rest_len = BLOCK_SIZE;
            }
        }

        // 用来处理不可存放 record 的情况，因为数据并未存入write，还需要接着调用
        if self.data_rest_len > 0 {
            self.add_process(data_byte)?;
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
        self.block_writer_rest_len -= data_byte.len();
        self.last_record_type = _type;
        info!("当前record {:?}",&record_header);
        info!("当前record type：{:?}",self.last_record_type);
        Ok(())
    }

}

/// WAL日志读取的引用结构
///
/// wal 文件始终只存在一个，服务器运行的过程中，
/// 服务器启动的时候log 文件夹中也只有一个，
/// 运行过程中创建新文件之前要先删除旧log 文件
#[derive(Debug)]
pub struct LogRecordRead {
    // 当前 log 文件的读柄
    block_reader: Option<BufReader<File>>,
    // pair
    kv_pair_byte: ByteVec,
    // 已读长度
    have_read_len: u64,
    // data 容器
    recovery_data: HashMap<String,KV>,
}
impl LogRecordRead {
    pub fn new() -> Result<Self> {
        let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
        create_dir_all(log_dir.clone())?;
        let mut block_reader = None;

        for entry in read_dir(log_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                block_reader = Some(BufReader::new(OpenOptions::new().read(true).open(path)?));
            }
        }

        Ok(LogRecordRead {
            block_reader,
            kv_pair_byte: ByteVec::new(),
            have_read_len: 0,
            recovery_data: HashMap::new()
        })
    }

    /// 读取整个log 文件
    pub fn read_log(&mut self) -> Result<()> {
        if let Some(reader) = self.block_reader.as_mut() {
            while self.have_read_len < reader.get_ref().metadata()?.len() {
                read_block_process(reader,
                                   self.have_read_len.borrow_mut(),
                                   self.recovery_data.borrow_mut())?;
            }
        };
        Ok(())
    }
}
/// 处理一个block的数据
fn read_block_process(block_reader: &mut BufReader<File>,
                      have_read_len: &mut u64,
                      recovery_data: &mut HashMap<String,KV>,) -> Result<()> {
    let mut buffer = [0; BLOCK_SIZE];
    // 自增 已读取 的长度
    *have_read_len += block_reader.read(&mut buffer)? as u64;

    let mut buffer = ByteVec::from(buffer);
    read_record_process(&mut buffer, recovery_data)?;
    Ok(())

}

/// 处理一条record
fn read_record_process(buffer: &mut ByteVec,
                       recovery_data: &mut HashMap<String,KV>) -> Result<()> {
    // 先读取header信息
    let mut rest_data = buffer.split_off(RECORD_HEADER_SIZE);
    let header = bincode::deserialize::<RecordHeader>(buffer.as_slice())?;
    println!("{:?}",header);

    // block空的尾部，此次block读取完毕
    if header._type == RecordType::None as u8 {
        Ok(())
    }
    // 读取 header 中data_len 的数据即可得到数据
    else if header._type == RecordType::Full as u8 {
        let mut value_data = rest_data.split_off(header.key_len as usize);
        let key = bincode::deserialize::<Key>(rest_data.as_slice())?;

        let mut rest_data = value_data.split_off(key.value_size as usize);
        let value = bincode::deserialize::<String>(value_data.as_slice())?;
        recovery_data.insert(key.get_sort_key(),KV::new(key,Some(value)));
        // 继续执行
        read_record_process(&mut rest_data, recovery_data)?;
        Ok(())
    }
    else if header._type == RecordType::First as u8 ||
        header._type == RecordType::Middle as u8 {
        // 往kv_pair_byte 中填充数据

        Ok(())
    }
    else {
        // RecordType::LastType 的情况
        // 往kv_pair_byte 中填充数据，并结束调用，可返回数据
        let a = "";
        Ok(())
    }
}

/// header 结构布局
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RecordHeader {
    pub checksum: u32,
    // 这里的指的是 Key 机构体的长度
    pub key_len: u32,
    pub _type: u8,
}
impl RecordHeader {
    pub fn new(checksum: u32, data_len: u32, _type: u8) -> Self {
        RecordHeader { checksum, key_len: data_len, _type }
    }
}
impl Default for RecordHeader {
    fn default() -> Self {
        RecordHeader {
            checksum: 0,
            key_len: 0,
            _type: RecordType::None as u8
        }
    }
}

/// record_type
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum RecordType {
    // 空数据header,或者默认的 type
    None,
    Full,
    // 分段的类型
    First,
    Middle,
    Last
}

/// 操作类型 可取：`Insert` `Update` `Delete`
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum CommandType {
    Insert,
    Update,
    Delete
}

/// internal_key = key + sequence + type
/// Key = internal_key_size + internal_key + value_size
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Key {
    key: String,
    sequence: i64,
    data_type: u8,
    value_size: u64
}
impl Key {
    /// 初始化 Key 实例和 value 数据
    pub fn new(key: String, value: Option<String>) -> (Self,Option<ByteVec>) {
        let sequence = Local::now().timestamp_millis();
        let (data_type, value_size, value) =  match value {
            Some(val) => {
                let value_byte = bincode::serialize(&val).unwrap();
                (DataType::Set, value_byte.len(), Some(value_byte))
            },
            None => {
                (DataType::Delete, 0, None)
            },
        };

        let key_len = key.as_bytes().len();
        (Key { key,
            sequence,
            data_type: data_type as u8,
            value_size: value_size as u64,
        },
         value)

    }

    /// 从 Key实例中 获取用于排序的key。
    pub fn get_sort_key(&self) -> String {
        format!("{}-{}", self.key, self.sequence)
    }

}

#[derive(Debug)]
pub struct KV {
    key: Key,
    value: Option<String>,
}
impl KV {
    pub fn new(key: Key,value: Option<String>) -> Self {
        KV { key, value }
    }
}


/// sst 数据类型
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum DataType {
    Delete,
    Set,
}


#[cfg(test)]
mod test {
    use super::*;
    use std::io::{Read, BufReader};
    use crate::common::fn_util::log_init;

    #[test]
    fn add_records_01_test() -> Result<()> {
        log_init();
        let mut log_record = LogRecordWrite::new()?;
        let mut str = String::new();
        File::open("a.txt")?.read_to_string(&mut str);
        let (data,mut value) = Key::new("a".to_string(),Some(str));
        log_record.add_records(&data,value.as_mut())?;
        Ok(())
    }

    #[test]
    fn add_records_02_test() -> Result<()> {
        log_init();
        let mut log_record = LogRecordWrite::new()?;
        let (data,mut value) = Key::new("a".to_string(),Some("aa".to_string()));
        log_record.add_records(&data, value.as_mut())?;

        let (data,mut value) = Key::new("b".to_string(),Some("bb".to_string()));
        log_record.add_records(&data, value.as_mut())?;
        let (data,mut value) = Key::new("c".to_string(),Some("cc".to_string()));
        log_record.add_records(&data, value.as_mut())?;
        Ok(())
    }

    #[test]
    fn read_test() -> Result<()> {
        let mut file = File::open("log/1638262824241.xlog")?;
        let mut reader = BufReader::new(file);

        let mut buffer = [0; BLOCK_SIZE];
        reader.read(&mut buffer)?;
        let mut buffer = ByteVec::from(buffer);
        let rest_data = buffer.split_off(RECORD_HEADER_SIZE);

        let header = bincode::deserialize::<RecordHeader>(buffer.as_slice())?;
        println!("{:?}",header);

        let _type = header._type;
        println!("{:?}",_type);
        if _type == RecordType::First as u8 {

        }


        Ok(())
    }

    #[test]
    fn test(){

    }

}