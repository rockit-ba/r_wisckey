//! log_record 数据模型
use serde_derive::{Serialize,Deserialize};
use anyhow::Result;
use crate::common::fn_util::{checksum, open_option_default, checksum_verify, gen_sequence};
use std::io::{BufWriter, Write, BufReader, Read};
use std::fs::{File, create_dir_all, read_dir, OpenOptions};
use log::{info, error};
use crate::common::types::ByteVec;
use std::collections::{HashMap, BTreeMap};
use std::env;
use crate::config::SERVER_CONFIG;
use chrono::Local;
use std::fmt::{Display, Formatter};
use std::ops::{DerefMut, Deref};
use std::cmp::Ordering;
use std::path::{PathBuf, Path};

/// block 大小：32 KB
pub const BLOCK_SIZE:usize = 1024 * 32;
/// checksum (4 bytes), _type(1 bytes), value_len(8 bytes)
pub const RECORD_HEADER_SIZE:usize = 4 + 1 + 8;
/// 日志文件达到预定大小（4MB），将转换为 sort table，并创建新的日志文件以供将来更新
pub const LOG_FILE_MAX_SIZE:usize = 1024 * 1024 * 4;

/// WAL日志写入的引用结构
#[derive(Debug)]
pub struct LogRecordWrite {
    /// 当前 log 文件的写句柄
    block_writer: BufWriter<File>,
    /// 上一次add_process的RecordType
    last_record_type: RecordType,
    /// 当前block剩余的空间，初始化就是满的 BLOCK_SIZE
    block_writer_rest_len: usize,
}
impl LogRecordWrite {

    /// 初始化 LogRecord 实体
    pub fn new() -> Result<Self> {
        // 当前 log 文件的写句柄
        let block_writer = gen_block_writer()?;
        Ok(LogRecordWrite {
            block_writer,
            last_record_type: RecordType::None,
            block_writer_rest_len: BLOCK_SIZE,
        })
    }

    /// 往 log 中添加 record
    ///
    /// 调用该方法之前初始化 Key，这里只负责写入
    ///
    /// return : 是否切换了新的 log ；engine 需要此信息去更改 memtable
    pub fn add_records(&mut self, data: &Key) -> Result<bool> {
        let mut is_new_log = false;
        // 当前log 文件大小校验,超过大小，创建新的log 文件写入
        if self.block_writer.buffer().len() >= LOG_FILE_MAX_SIZE {
            self.block_writer = gen_block_writer()?;
            is_new_log = true;
        }

        let mut data_byte = data.encode();
        info!("data:{:?}",data);
        self.add_process(&mut data_byte)?;
        Ok(is_new_log)
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
                info!("data_free_size: {}",data_free_size);
                // 如果当前的数据在当前的block中放不下
                if data_byte.len() > data_free_size {
                    // 根据data_free_size 切割
                    let mut rest_data_byte = data_byte.split_off(data_free_size);

                    match self.last_record_type {
                        RecordType::None | RecordType::Full | RecordType::Last => {
                            info!("First len {}",data_byte.len());
                            self.write_for_type(data_byte, RecordType::First)?;
                        },
                        RecordType::First | RecordType::Middle => {
                            info!("Middle len {}",data_byte.len());
                            self.write_for_type(data_byte, RecordType::Middle)?;
                        },
                    };
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
                info!("当前record 空header");
            }
            Ordering::Less => {
                // 使用 [0_u8;block_free_size] 填充
                self.block_writer.write_all(vec![0_u8;self.block_writer_rest_len].as_slice())?;
                self.block_writer.flush()?;
                self.block_writer_rest_len = BLOCK_SIZE;
                info!("当前record [0_u8;block_free_size] 填充");
            }
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
                                              _type.clone() as u8,
                                              data_byte.len() as u64);

        let mut header_byte = bincode::serialize(&record_header)?;
        header_byte.append(data_byte);

        self.block_writer.write_all(header_byte.as_slice())?;
        // 每写一条record就flush
        self.block_writer.flush()?;
        // 注意，不能直接重置为 BLOCK_SIZE，因为它可能是不满 block的
        self.block_writer_rest_len -= header_byte.len();
        // 如果为 0 ，重置为满 block，重新开始写
        if self.block_writer_rest_len == 0 {
            self.block_writer_rest_len = BLOCK_SIZE;
        }
        self.last_record_type = _type;
        info!("当前record {:?}",&record_header);
        Ok(())
    }

}

/// 获取一个新的log 文件写句柄
fn gen_block_writer() -> Result<BufWriter<File>> {
    let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
    create_dir_all(log_dir.clone())?;

    let file_name = format!("{}.{}", gen_sequence(), SERVER_CONFIG.log_file_extension);
    let log_file = open_option_default(log_dir.join(file_name.as_str()))?;
    // 当前 log 文件的写句柄
    Ok(BufWriter::with_capacity(BLOCK_SIZE,log_file))
}

/// WAL日志读取的引用结构
///
/// wal 文件始终只存在一个，服务器运行的过程中，
/// 服务器启动的时候log 文件夹中也只有一个，
/// 运行过程中创建新文件之前要先删除旧log 文件
#[derive(Debug)]
pub struct LogRecordRead {
    /// 当前 log 文件的读柄
    block_reader: Option<BufReader<File>>,
    /// value 值的字节数组，将会在读的过程中累积
    ///
    /// 得到完整的 value 之后将会清空该字节数组（服务于跨block读取）
    value_byte: ByteVec,
    /// 已读长度
    have_read_len: u64,
    /// data 容器
    recovery_data: BTreeMap<String, Key>,
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
            value_byte: ByteVec::new(),
            have_read_len: 0,
            recovery_data: BTreeMap::new()
        })
    }

    /// 读取整个log 文件
    pub fn read_log(&mut self) -> Result<()> {
        if let Some(reader) = self.block_reader.as_mut() {
            while self.have_read_len < reader.get_ref().metadata()?.len() {
                read_block_process(reader,
                                   &mut self.have_read_len,
                                   &mut self.recovery_data,
                                   &mut self.value_byte)?;
            }
            info!("读取完毕：have_read_len {}",self.have_read_len);
        };
        Ok(())
    }
}
/// 处理一个block的数据
fn read_block_process(block_reader: &mut BufReader<File>,
                      have_read_len: &mut u64,
                      recovery_data: &mut BTreeMap<String, Key>,
                      value_byte: &mut ByteVec) -> Result<()>
{
    let mut buffer = [0; BLOCK_SIZE];
    // 自增 已读取 的长度
    *have_read_len += block_reader.read(&mut buffer)? as u64;
    info!("############### 读取block ###############");
    let mut buffer = ByteVec::from(buffer);

    read_record_process(&mut buffer, recovery_data, value_byte)
}

/// 处理一条record
fn read_record_process(buffer: &mut ByteVec,
                       recovery_data: &mut BTreeMap<String, Key>,
                       value_byte: &mut ByteVec) -> Result<()>
{
    if buffer.len() < RECORD_HEADER_SIZE {
        return Ok(());
    }
    // 先读取header信息
    let mut rest_data = buffer.split_off(RECORD_HEADER_SIZE);
    let header = bincode::deserialize::<RecordHeader>(buffer.as_slice())?;
    info!("header：{:?}",header);

    // block空 header 的尾部，此次 block 读取完毕
    if header._type == RecordType::None as u8 {
        Ok(())
    }
    // 读取 header 中data_len 的数据即可得到数据
    else if header._type == RecordType::Full as u8 {
        let mut other_data = rest_data.split_off(header.value_len as usize);
        if !checksum_verify(rest_data.as_slice(), header.checksum) {
            error!("checksum 校验失败: {:?}",&header);
        }else {
            let key_test = Key::decode(&mut rest_data)?;
            recovery_data.insert(key_test.get_sort_key(),key_test);
        }
        // 继续执行
        read_record_process(&mut other_data, recovery_data, value_byte)?;
        Ok(())
    }
    else if header._type == RecordType::First as u8 {
        // 对于first 来说它不需要知道value ，只需要把剩下的直接拼接到value_byte 即可
        if !checksum_verify(rest_data.as_slice(), header.checksum) {
            error!("checksum 校验失败: {:?}",&header);
        }else {
            value_byte.append(&mut rest_data);
        }
        Ok(())
    }
    // 往value_byte 中填充数据
    else if header._type == RecordType::Middle as u8 {
        // 对于Middle 来说同样它不需要知道value ，只需要把剩下的直接拼接到value_byte 即可
        if !checksum_verify(rest_data.as_slice(), header.checksum) {
            error!("checksum 校验失败: {:?}",&header);
        }else {
            value_byte.append(&mut rest_data);
        }
        Ok(())
    }
    // RecordType::LastType 的情况
    else {
        // 往value_byte 中填充数据,并且之后可以获取完整的value
        // 对于last 来说 它后面可能还会有数据，所以它需要value_size 来确定截取的长度
        let mut other_data = rest_data.split_off(header.value_len as usize);
        if !checksum_verify(rest_data.as_slice(), header.checksum) {
            error!("checksum 校验失败: {:?}",&header);
        }else {
            value_byte.append(&mut rest_data);
            let key_test = Key::decode(value_byte)?;
            recovery_data.insert(key_test.get_sort_key(), key_test);
            // 清空 value_byte
            value_byte.clear();
        }
        // 继续执行
        info!("last 读取完毕");
        read_record_process(&mut other_data, recovery_data, value_byte)?;
        Ok(())
    }
}

/// header 结构布局
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RecordHeader {
    pub checksum: u32,
    pub _type: u8,
    pub value_len: u64,
}
impl RecordHeader {
    pub fn new(checksum: u32, _type: u8, value_len: u64) -> Self {
        RecordHeader { checksum, value_len, _type }
    }
}
impl Default for RecordHeader {
    fn default() -> Self {
        RecordHeader {
            checksum: 0,
            value_len: 0,
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

/// 操作类型 可取：`Insert` `Update` `Delete`，针对用户命令解析
///
/// 不同于DataType 中的 type。
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum CommandType {
    Insert,
    Update,
    Delete
}

/// internal_key = key + sequence + type
///
/// Key = internal_key_size + internal_key + value_size + value
#[derive(Debug, Clone)]
pub struct Key {
    internal_key_size: u64,
    key: String,
    sequence: i64,
    data_type: u8,
    value_size: u64,
    value: String,
}
impl Key {
    pub fn new(key: String,
               value: String,
               data_type: DataType) -> Self{
        let sequence = gen_sequence(); // 8
        let data_type = data_type as u8; // 1
        let value_size = value.as_bytes().len() as u64; // 8
        let internal_key_size = key.as_bytes().len() as u64 + 9_u64;
        Key {
            internal_key_size,
            key,
            sequence,
            data_type,
            value_size,
            value
        }
    }

    /// 从 Key实例中 获取用于排序的key。
    pub fn get_sort_key(&self) -> String {
        format!("{}-{}", self.key, self.sequence)
    }

    pub fn encode(&self) -> ByteVec {
        let mut buf = ByteVec::new();

        let mut internal_key_size_byte = self.internal_key_size.to_le_bytes().to_vec();
        let mut key_byte = self.key.as_bytes().to_vec();
        let mut sequence_byte = self.sequence.to_le_bytes().to_vec();
        let mut data_type_byte = self.data_type.to_le_bytes().to_vec();
        let mut value_size_byte = self.value_size.to_le_bytes().to_vec();
        let mut value_byte = self.value.as_bytes().to_vec();

        buf.append(&mut internal_key_size_byte);
        buf.append(&mut key_byte);
        buf.append(&mut sequence_byte);
        buf.append(&mut data_type_byte);
        buf.append(&mut value_size_byte);
        buf.append(&mut value_byte);

        buf.clone()
    }

    pub fn decode(content: &mut ByteVec) -> Result<Self>{
        let mut rest_content = content.split_off(8_usize);
        let internal_key_size = bincode::deserialize::<u64>(content.as_slice())?;

        // 切割出 key + sequence + data_type
        let mut value_content = rest_content.split_off(internal_key_size as usize);
        let key_rest_content = rest_content.split_off(rest_content.len() - 9_usize);
        let key = String::from_utf8(rest_content)?;
        let (sequence_byte,data_type_byte) = key_rest_content.split_at(8_usize);
        let sequence = bincode::deserialize::<i64>(sequence_byte)?;
        let data_type = bincode::deserialize::<u8>(data_type_byte)?;

        let value_byte = value_content.split_off(8_usize);
        let value_size = bincode::deserialize::<u64>(value_content.as_slice())?;
        let value = String::from_utf8(value_byte)?;
        Ok(
            Key {
                internal_key_size,
                key,
                sequence,
                data_type,
                value_size,
                value
            }
        )

    }
}

/// sst 数据类型
///
/// 正对存储引擎本身，不同于 CommandType
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum DataType {
    Delete,
    Set,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::{Read, BufReader};
    use crate::common::fn_util::{log_init, SEQUENCE};
    use serde::Deserialize;
    use std::convert::{TryFrom, TryInto};

    #[test]
    fn add_records_01_test() -> Result<()> {
        log_init();
        // 垮block 数据 测试
        let mut log_record = LogRecordWrite::new()?;
        let mut str = String::new();
        File::open("a.txt")?.read_to_string(&mut str);
        let key_test = Key::new("a".to_string(), str, DataType::Set);
        log_record.add_records(&key_test)?;
        Ok(())
    }

    #[test]
    fn add_records_02_test() -> Result<()> {
        log_init();
        // 跨block 和正常 数据 测试
        let mut log_record = LogRecordWrite::new()?;
        let key_test = Key::new("b".to_string(), "bb".to_string(), DataType::Set);
        log_record.add_records(&key_test)?;

        let mut str = String::new();
        File::open("a.txt")?.read_to_string(&mut str);
        let key_test = Key::new("a".to_string(), str, DataType::Set);
        log_record.add_records(&key_test)?;

        let key_test = Key::new("d".to_string(), "dd".to_string(), DataType::Set);
        log_record.add_records(&key_test)?;
        let key_test = Key::new("e".to_string(), "ee".to_string(), DataType::Set);
        log_record.add_records(&key_test)?;
        Ok(())
    }

    #[test]
    fn add_records_03_test() -> Result<()> {
        log_init();
        let mut log_record = LogRecordWrite::new()?;
        let data = vec![
            ("a".to_string(),"bb".to_string()),("a".to_string(),"bb".to_string()),
            ("a".to_string(),"bb".to_string()),("b".to_string(),"bb".to_string()),
            ("c".to_string(),"cc".to_string()),("d".to_string(),"dd".to_string()),
            ("e".to_string(),"ee".to_string()),("f".to_string(),"ff".to_string()),
        ];
        data.iter().for_each(|(key, value)| {
            let key_test = Key::new(key.clone(), value.clone(), DataType::Set);
            log_record.add_records(&key_test).unwrap();
        });
        Ok(())
    }

    #[test]
    fn read_test() -> Result<()> {
        log_init();
        let mut reader = LogRecordRead::new()?;
        reader.read_log()?;
        let data = reader.recovery_data;
        println!("{:?}",data);
        Ok(())
    }

    #[test]
    fn test() {


    }
}