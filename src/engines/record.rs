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

/// record 键值对
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct KVPair {
    pub key: String,
    // none 将占一个字节
    pub value: Option<String>,
}
impl KVPair {
    pub fn new(key: String, value: Option<String>) -> Self {
        KVPair { key, value }
    }
}

#[cfg(test)]
mod test {
    use crate::engines::record::KVPair;
    use crate::common::fn_util::*;

    #[test]
    fn test() {
        log_init();
        let kv = KVPair::new("aa".to_string(),None);
        let byte_kv = bincode::serialize(&kv).unwrap();
        log::info!("{}",byte_kv.len());
        let kv = bincode::deserialize::<KVPair>(byte_kv.as_slice()).unwrap();
        log::info!("{:?}",kv);
        let str = "aa".to_string();
        let byte_str = bincode::serialize(&str).unwrap();
        log::info!("{}",byte_str.len());
    }
}
