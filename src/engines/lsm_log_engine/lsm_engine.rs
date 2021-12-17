//! LSM-tree 存储引擎核心

#![allow(dead_code)]

use anyhow::Result;
use crossbeam_skiplist::SkipMap;
use log::info;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{BufWriter, Write};
use std::path::{PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::engines::lsm_log_engine::level::LevelDir;
use crate::engines::lsm_log_engine::mem::MemTables;
use crate::engines::lsm_log_engine::wal_log::{DataType, Key, LogRecordRead, LogRecordWrite};
use crate::engines::Scans;
use crate::KvsEngine;

/// minor-thread name
pub const MINOR_THREAD: &str = "minor-thread";

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
        })
    }
}
impl KvsEngine for LsmLogEngine {
    /// 用户的set操作
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let internal_key = Key::new(key.to_string(), value.to_string(), DataType::Set);

        // 写 WAL 的逻辑先于其他逻辑，这里失败就会返回用户此次操作失败
        // is_new_log: 是否开启了新的日志文件
        if let Some(new_log_path) = self.wal_writer.add_records(&internal_key)? {
            info!("开启了新的日志文件");
            // 如果开启了新的日志文件，
            // 1 表示当前的key已经被添加到 新的log文件中了，需要调换table,
            // 调换 两个table的状态（只是修改状态不涉及其它修改）
            self.mem_tables.exchange();
            // 2 同时当前的 memtable 就需要 flush
            minor_compact(
                self.mem_tables.imu_table().unwrap().table.clone(),
                Arc::new(Mutex::new(new_log_path)))?;
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
fn minor_compact(
    imu_table: Arc<SkipMap<String, Key>>,
    write_log_path: Arc<Mutex<PathBuf>>
) -> Result<()> {
    thread::Builder::new()
        .name(MINOR_THREAD.to_string())
        .spawn(move || -> Result<()> {
            info!("当前imu_table len{}", &imu_table.len());
            // TODO  测试代码
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open("b.txt")?;
            // 每次一次切换写入一个 |#| 块
            file.write_all(b"|#|")?;
            file.flush()?;

            imu_table.clear();
            // 之后删除该imu_table 对应的log 文件
            remove_file(write_log_path.lock().unwrap().as_path())?;
            Ok(())
        })?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::fn_util::log_init;

    #[test]
    fn test_01() -> Result<()> {
        log_init();
        let mut engine = LsmLogEngine::open()?;
        // 83886.08
        for _ in 0..283880 {
            engine.set("测试", "测试")?;
        }
        println!("{:?}", &engine);

        Ok(())
    }
}
