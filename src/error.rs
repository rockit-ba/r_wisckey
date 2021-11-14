use failure::Fail;
use std::io;
use std::string::FromUtf8Error;


//重新定义自己的result 类型，避免再程序中到处写  Result<T,KvsError>
pub type Result<T> = std::result::Result<T, WiscError>;

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

    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),

    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),

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

impl From<FromUtf8Error> for WiscError {
    fn from(_err: FromUtf8Error) -> Self {
        WiscError::Utf8(_err)
    }
}


