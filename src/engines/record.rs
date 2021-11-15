//! record 数据模型
use serde_derive::{Serialize,Deserialize};

pub const RECORD_HEADER_SIZE:usize = 9;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Record {
    header: RecordHeader,
    data: KVPair,
}
impl Record {
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
    // 1 set  0  delete
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

/// 键值对record
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}
impl KVPair {
    pub fn new(key: String, value: String) -> Self {
        KVPair { key, value }
    }
}
