//! record 数据模型
use serde_derive::{Serialize,Deserialize};

/// record header的大小，固定值
pub const RECORD_HEADER_SIZE:usize = 9;

/// 存储磁盘的完整实体
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Record {
    header: RecordHeader,
    data: KVPair,
}
impl Record {
    #[allow(dead_code)]
    pub fn new(header: RecordHeader, data: KVPair) -> Self {
        Record {
            header,
            data
        }
    }
}

/// header布局
///
/// command_type+checksum+key_len+val_len ，1+4+4=9byte
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RecordHeader {
    // CommandType 取值
    pub command_type: u8,
    pub checksum: u32,
    pub data_len: u32
}
impl RecordHeader {
    pub fn new(command_type: u8,checksum: u32,data_len: u32) -> Self {
        RecordHeader {
            command_type,
            checksum,
            data_len,
        }
    }
}

/// 操作类型 可取：`Insert` `Update` `Delete`
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum CommandType {
    Insert = 1,
    Update = 2,
    Delete = 0
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
    use crate::common::fn_util::*;

    #[test]
    fn test() {
        log_init();
    }
}
