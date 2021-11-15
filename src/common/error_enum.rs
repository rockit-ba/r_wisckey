//! 自定义error
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WiscError {
    #[error("data corruption encountered (expected {saved_checksum:?}, got {checksum:?})")]
    DataCorruption {
        checksum: u32,
        saved_checksum: u32,
    },

    #[error("key: [{0}] not found")]
    KeyNotFound(String),
}