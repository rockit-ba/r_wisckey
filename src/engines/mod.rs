use std::ops::Range;
use serde_derive::{Deserialize, Serialize};

pub use lsm_log_engine::lsm_engine::LsmLogEngine;
pub mod lsm_log_engine;

pub trait KvsEngine  {
    /// 设置字符串键值对
    ///
    /// 如果key 已经存在，则之前的对应的value将被新的覆盖
    fn set(&mut self, key: &str, value: &str) -> anyhow::Result<()>;

    /// 根据key 获取一个 value
    ///
    /// 如果 key 不存在返回 none
    fn get(&self, key: &str) -> anyhow::Result<Option<String>>;

    fn scan(&self, range: Scans) -> anyhow::Result<Option<Vec<String>>>;

    /// 删除给定的 key
    ///
    /// 如果给定的key 不存在将返回 `KvsError::KeyNotFound`
    fn remove(&mut self, key: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Scans(Range<String>);
