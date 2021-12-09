//! LSM-tree 存储引擎核心

#![allow(dead_code)]

use anyhow::Result;
use std::io::BufWriter;
use std::fs::{File};
use std::{thread};
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

use crate::KvsEngine;
use crate::engines::{Scans};
use crate::engines::lsm_log_engine::level::LevelDir;
use crate::engines::lsm_log_engine::wal_log::{ LogRecordWrite, LogRecordRead, Key, DataType};
use crate::engines::lsm_log_engine::mem::MemTables;

/// minor-thread name
pub const MINOR_THREAD:&str = "minor-thread";

/// 更新操作最终在lsm看来只有两种操作：set和 delete
///
/// 在执行用户的 update操作之前需要先执行get操作。
/// 存在则执行，不存在则返回用户执行insert操作
#[derive(Debug)]
pub struct LsmLogEngine {
    /// 接收用户的命令之后需要写 WAL日志，因此
    wal_writer: LogRecordWrite,
    /// 故障恢复时需要读取 WAL日志
    wal_reader: LogRecordRead,
    /// MemTable,因为我们需要保持数据的有序性，
    mem_tables: MemTables,
    /// mem_table 不可变之后将刷入 level-0 SSTable
    level_0_writer: BufWriter<File>,

}
impl LsmLogEngine {
    pub fn open() -> Result<Self> {
        // 初始化 wal_writer 和 wal_reader
        let wal_writer = LogRecordWrite::new()?;
        let wal_reader = LogRecordRead::new()?;
        // 初始化 mem_table
        let mem_tables = MemTables::new();

        // 初始化 sst_writer，从level_0文件夹中查找，因为 minor compression 直接
        // 刷到level_0层级中的sst文件中
        let level_0_writer = LevelDir::new(0).init_level_0_writer()?;

        Ok(LsmLogEngine {
            wal_writer,
            wal_reader,
            mem_tables,
            level_0_writer,
        }
        )
    }

}
impl KvsEngine for LsmLogEngine {
    /// 用户的set操作
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let internal_key = Key::new(key.to_string(),value.to_string(),DataType::Set);
        // 写 WAL 的逻辑先于其他逻辑，这里失败就会返回用户此次操作失败
        let is_new_log = self.wal_writer.add_records(&internal_key)?;
        if is_new_log {
            // 如果为true ，表示当前的key已经被添加到 新的log文件中了，需要调换table
            // 调换 两个table的状态
            self.mem_tables.exchange();
            // 当前的memtable就需要flush
            minor_compact(self.mem_tables.imu_table().unwrap().table.clone())?;
        }
        // 将数据写入内存表
        self.mem_tables.add_record(&internal_key);
        Ok(())
    }

    #[warn(unused_variables)]
    fn get(&self, _key: &str) -> Result<Option<String>> {
        todo!()
    }

    #[warn(unused_variables)]
    fn scan(&self, _range: Scans) -> Result<Option<Vec<String>>> {
        todo!()
    }

    #[warn(unused_variables)]
    fn remove(&mut self, _key: &str) -> Result<()> {
        todo!()
    }
}


/// 将当前的 imu_table flush到 level-0
pub fn minor_compact(imu_table: Arc<SkipMap<String,Key>>) -> Result<()> {
    thread::Builder::new()
        .name(MINOR_THREAD.to_string())
        .spawn(move || -> Result<()> {

            // TODO
            imu_table.clear();
            Ok(())

        })?;

    Ok(())
}



#[cfg(test)]
mod test {

    #[test]
    fn test() {

    }

}