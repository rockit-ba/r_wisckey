//! LSM-tree 存储引擎核心

use anyhow::Result;
use crate::KvsEngine;
use crate::engines::{Scans, write_ahead_log};
use std::sync::{Arc, Mutex};
use std::io::BufWriter;
use std::fs::{File};
use std::sync::atomic::{AtomicU64};
use crate::config::SERVER_CONFIG;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::path::PathBuf;
use crate::engines::lsm_log_engine::wal_log::{CommandType, LogRecordWrite, LogRecordRead};
use crate::common::fn_util::init_file_writer;
use crate::engines::lsm_log_engine::mem::MemTables;

#[derive(Debug)]
pub struct LsmLogEngine {
    // 首先接收用户的命令之后需要写 WAL日志，因此
    wal_writer: LogRecordWrite,
    wal_reader: LogRecordRead,
    // 接着需要写入 MemTable,因为我们需要保持数据的有序性，
    mem_tables: MemTables,
    // mem_table 不可变之后将刷入 SSTable
    sst_writer: BufWriter<File>,
    sst_write_name: AtomicU64,

}
impl LsmLogEngine {
    pub fn open() -> Result<Self> {
        // 初始化 wal_writer
        let wal_writer = LogRecordWrite::new()?;
        let wal_reader = LogRecordRead::new()?;
        // 初始化 mem_table
        let mem_tables = MemTables::new();

        // 初始化 sst_writer，从level_0文件夹中查找，因为 minor compression 直接
        // 刷到level_0层级中的sst文件中
        let level_0 = LevelDir::new(0);
        let (sst_writer,sst_write_name) = level_0.init()?;

        Ok(LsmLogEngine {
            wal_writer,
            wal_reader,
            mem_tables,
            sst_writer,
            sst_write_name
        })
    }

}
impl KvsEngine for LsmLogEngine {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {

        // 写 WAL 的逻辑先于其他逻辑，这里失败就会返回用户此次操作失败

        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        todo!()
    }

    fn scan(&self, range: Scans) -> Result<Option<Vec<String>>> {
        todo!()
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        todo!()
    }
}

/// LevelDir抽象
pub struct LevelDir (String,u8);
impl LevelDir {
    pub fn new(level: u8) -> Self {
        LevelDir { 0: "level_".to_string(), 1: level }
    }
    pub fn to_path(&self) -> Result<PathBuf> {
        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        Ok(data_dir.join(format!("{}{}",self.0,self.1)))
    }
    /// 初始化 创建并返回当前的 writer
    pub fn init(&self) -> Result<(BufWriter<File>,AtomicU64)> {
        let path = self.to_path()?;
        let (sst_writer,sst_write_name) = init_file_writer(path,
                         SERVER_CONFIG.data_file_extension.as_str(),
                         SERVER_CONFIG.data_file_suffix.as_str(),
                         SERVER_CONFIG.level_0_max_size)?;

        Ok((sst_writer,sst_write_name))
    }

}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {

    }

}