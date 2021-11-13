//! r_wisckey 的API抽象
//! 共提供四种 接口
//! get, set , delete , scan


mod log_engine;

use crate::Result;
use std::ops::{Range};

pub trait DbEngine: Clone + Send + 'static  {
    /// 设置字符串键值对
    ///
    /// 如果key 已经存在，则之前的对应的value将被新的覆盖
    fn set(&self, key: String, value: String) -> Result<()>;

    /// 根据key 获取一个 value
    ///
    /// 如果 key 不存在返回 none
    fn get(&self, key: String) -> Result<Option<String>>;

    /// 范围扫描，get 本质也是通过此方法来实现的
    fn scan(&self, range: Range<String>) -> Result<Option<Vec<String>>>;

    /// 删除给定的 key
    ///
    /// 如果给定的key 不存在将返回 `KvsError::KeyNotFound`
    fn remove(&self, key: String) -> Result<()>;
}