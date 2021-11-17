//! 配置文件解析
use lazy_static::lazy_static;
use anyhow::Result;
use serde_derive::Deserialize;
use std::env::current_dir;
use std::path::Path;
use crate::common::error_enum::WiscError;
/// 配置文件名
const SERVER_CONFIG_FILE:&str = "server.yml";
/// 配置文件基础目录
const CONFIG_BASE_DIR:&str = "config";

// 加载全局 ServerConfig
lazy_static! {
    pub static ref SERVER_CONFIG: ServerConfig = {
        ServerConfig::new().unwrap()
    };
}
/// server.yml 解析类
///
/// 字段含义查看 config/server.yml 文件
#[derive(Debug,Deserialize)]
pub struct ServerConfig {
    pub data_dir: String,
    pub data_file_suffix: String,
    pub data_file_extension: String,
    pub file_max_size: u64,
    pub compress_threshold: usize,
    pub compress_interval: u64,
}
impl ServerConfig {
    fn new() -> Result<Self> {
        let mut c = config::Config::new();
        let path = current_dir()?
            .join(CONFIG_BASE_DIR)
            .join(Path::new(SERVER_CONFIG_FILE));
        if !path.exists() {
            return Err(
                anyhow::Error::from(
                    WiscError::FileNotFound(String::from(path.to_str().unwrap())))
            )
        }
        let file = path.to_str().unwrap();
        c.merge(config::File::with_name(file))?;
        Ok(c.try_into()?)
    }
}
