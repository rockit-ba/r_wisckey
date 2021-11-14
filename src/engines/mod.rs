mod base_log;

pub use self::base_log::LogEngine;

use std::ops::Range;
use serde_derive::{Deserialize,Serialize};

pub trait KvsEngine: Clone + Send + 'static  {
    /// 设置字符串键值对
    ///
    /// 如果key 已经存在，则之前的对应的value将被新的覆盖
    fn set(&self, key: String, value: String) -> anyhow::Result<()>;

    /// 根据key 获取一个 value
    ///
    /// 如果 key 不存在返回 none
    fn get(&self, key: String) -> anyhow::Result<Option<String>>;

    fn scan(&self, range: Scans) -> anyhow::Result<Option<Vec<String>>>;

    /// 删除给定的 key
    ///
    /// 如果给定的key 不存在将返回 `KvsError::KeyNotFound`
    fn remove(&self, key: String) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Scans(Range<String>);
impl From<String> for Scans {
    fn from(_range: String) -> Self {
        let _range: Vec<String> =  _range.split('-').map(|ele| {
            String::from(ele)
        }).collect();
        Scans { 0: (_range.get(0).unwrap().clone().._range.get(1).unwrap().clone()) }
    }
}