use thiserror::Error;

#[derive(Error, Debug)]
pub enum WiscError {
    #[error("Invalid header (expected {expected:?}, got {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },

    #[error("key: [{0}] not found")]
    KeyNotFound(String),
}