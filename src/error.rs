//! 内部自定义 Result 处理error
//! 避免再程序中到处写  Result<T,WiscError>

use failure::Fail;
use std::io;


//重新定义自己的result 类型，
pub type Result<T> = std::result::Result<T,WiscError>;

/// 所有的错误类型
#[derive(Fail, Debug)]
pub enum WiscError {
    #[fail(display = "{}", _0)]
    IO(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    Serde(#[cause] bincode::Error),

    #[fail(display = "key not found")]
    KeyNoFound,

    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    /// string message 定义 Error
    #[fail(display = "{}", _0)]
    StringError(String),
}

impl From<io::Error> for WiscError {
    fn from(_err: io::Error) -> Self {
        WiscError::IO(_err)
    }
}

impl From<bincode::Error> for WiscError {
    fn from(_err: bincode::Error) -> Self {
        WiscError::Serde(_err)
    }
}