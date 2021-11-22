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
    /// 存放数据文件的基础目录
    pub data_dir: String,
    /// 数据文件的后缀名
    pub data_file_suffix: String,
    /// 数据文件的扩展名
    pub data_file_extension: String,
    /// 据文件最大容量
    pub file_max_size: u64,
    /// 压缩阈值
    pub compress_threshold: usize,
    /// 压缩线程执行的频率，单位：秒。
    pub compress_interval: u64,
    /// wisc_server 默认的启动地址
    pub server_addr: String,
    /// 命令行历史存放文件
    pub command_history: String,
    /// write_buf_max_size
    pub write_buf_max_size: usize,
    /// wal 日志存储目录
    pub wal_dir: String,
    pub log_file_suffix: String,
    pub log_file_extension: String,
    pub log_file_max_size: usize,
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
