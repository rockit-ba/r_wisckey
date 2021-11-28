use std::ops::Range;

use serde_derive::{Deserialize, Serialize};

pub use base_log_engine::base_log::LogEngine;
use std::io::{BufWriter, Write};
use std::sync::atomic::{Ordering, AtomicU64};
use crate::common::fn_util::{open_option_default, get_file_path, sorted_gen_list, init_file_writer};
use crate::engines::base_log_engine::record::CommandType;
use std::sync::{Arc, Mutex};
use anyhow::Result;
use std::fs::File;
use std::{env, fs};
use crate::config::SERVER_CONFIG;


mod base_log_engine;
mod lsm_log_engine;

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
// impl From<String> for Scans {
//     fn from(_range: String) -> Self {
//         let _range: Vec<String> =  _range.split('-').map(|ele| {
//             String::from(ele)
//         }).collect();
//         Scans { 0: (_range.get(0).unwrap().clone().._range.get(1).unwrap().clone()) }
//     }
// }

pub fn init_wal() -> Result<(BufWriter<File>, AtomicU64)>{
    // 初始化 wal_writer
    let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
    let (wal_writer,wal_write_name) = init_file_writer(log_dir,
                     SERVER_CONFIG.log_file_extension.as_str(),
                     SERVER_CONFIG.log_file_suffix.as_str(),
                     SERVER_CONFIG.log_file_max_size)?;

    Ok((wal_writer,wal_write_name))
}


/// 写 WAL 日志。
///
/// 就传统的关系型数据库，通常来说我们会在一次事务提交之后对`log_buf`中的数据进行 flush。
/// 当此次操作的 WAL 日志持久化之后我们才返回客户端此次操作success。
///
/// 另外的可选方案是定时flush log_buf，例如每秒flush一次，极端情况下，我们可能丢失一秒内
/// 客户端的操作数据。
///
/// 如果我们需要数据库的基本ACID特性，我们将不会选择定时，而是选择 用户提交事务即持久化。
/// 用户可能一次进行单条数据修改，也可能多个。
///
/// 因此现在我们选择：每次客户端的数据修改操作（除了 查询操作）都进行 log 记录并 flush。
///
///PS：这里可能显得有些多余，因为我们可以选择直接在data_file存储中进行每次flush，因为目前来说
/// 我们的data_file 也是append 写入的。但是别忘了，我们将要在不就的将来实现 LSM 模型存储，在
/// LSM 模型的data_file（SSTable） 中,数据将不会按照用户的写入顺序单条append 写入，因此WAL
/// 的存在必不可少。
pub fn write_ahead_log(wal_writer: Arc<Mutex<BufWriter<File>>>,
                       wal_write_name: &AtomicU64,
                       command_type: &CommandType,
                       key: &str,
                       value: Option<&str>) -> Result<()> {
    let log_str = match command_type {
        CommandType::Insert => {
            format!("{} {} {};","insert",key,value.unwrap())
        },
        CommandType::Delete =>{
            format!("{} {};","delete",key)
        },
        CommandType::Update => {
            format!("{} {} {};","update",key,value.unwrap())
        }
    };

    let log_dir = env::current_dir()?.join(&SERVER_CONFIG.wal_dir);
    let last_log_file = open_option_default(get_file_path(log_dir.as_path(),
                                                          wal_write_name.load(Ordering::SeqCst),
                                                          SERVER_CONFIG.log_file_suffix.as_str()))?;
    {
        let mut _wal_writer = wal_writer.lock().unwrap();
        if last_log_file.metadata()?.len() >= SERVER_CONFIG.log_file_max_size as u64 {
            wal_write_name.fetch_add(1,Ordering::SeqCst);
            *_wal_writer = BufWriter::new(
                open_option_default(get_file_path(log_dir.as_path(),
                                                  wal_write_name.load(Ordering::SeqCst),
                                                  SERVER_CONFIG.log_file_suffix.as_str()))?
            );

        }else {
            *_wal_writer = BufWriter::new(last_log_file);
        }

        _wal_writer.write_all(log_str.as_bytes())?;
        _wal_writer.flush()?;
    }

    Ok(())
}