//! 自定义error
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

/// 自定义内部错误
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum WiscError {
    /// checksum值不一致
    #[error("data corruption encountered (expected {saved_checksum:?}, got {checksum:?})")]
    DataCorruption { checksum: u32, saved_checksum: u32 },

    #[error("file: [{0}] not found!")]
    FileNotFound(String),

    #[error("key: [{0}] not exist!")]
    KeyNotExist(String),

    #[error("insert fail :key: [{0}] existed, consider update that")]
    KeyExist(String),

    #[error("SocketAddr parser fail !")]
    SocketAddrParserFail,
}
