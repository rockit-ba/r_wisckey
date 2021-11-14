//! record 数据模型
use serde_derive::{Serialize,Deserialize};

pub const RECORD_HEADER_SIZE:usize = 13;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Record {
    header: RecordHeader,
    data: KVPair,
}
/// header布局
///
/// command_type+checksum+key_len+val_len ，1+4+4+4=13byte
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RecordHeader {
    pub command_type: u8,
    pub checksum: u32,
    pub key_len: u32,
    pub value_len: u32
}
/// 键值对record
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}
