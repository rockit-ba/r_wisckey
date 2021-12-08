//! LSM-tree 存储引擎核心
#![allow(dead_code)]

use anyhow::Result;
use crate::KvsEngine;
use crate::engines::{Scans};
use std::io::BufWriter;
use std::fs::{File, read_dir, OpenOptions};
use crate::config::SERVER_CONFIG;
use std::{env, thread};
use std::path::PathBuf;
use crate::engines::lsm_log_engine::wal_log::{ LogRecordWrite, LogRecordRead, Key, DataType};
use crate::common::fn_util::{open_option_default, get_file_path, gen_sequence};
use crate::engines::lsm_log_engine::mem::MemTables;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// minor-thread name
pub const MINOR_THREAD:&str = "minor-thread";

/// LEVEL_0 单个文件的大小 1M
pub const LEVEL_0_FILE_MAX_SIZE:u64 = 1024 * 1024;
/// LEVEL_0 层所有文件的最大数量
pub const LEVEL_0_FILE_MAX_NUM:usize = 4;

/// 非 LEVEL_0 单个文件的大小 2M
pub const LEVEL_FILE_MAX_SIZE:usize = 1024 * 1024 * 2;
/// 换句话说就是level-1层的所有文件最大个数，也就是level-1总大小在 10M
pub const LEVEL_FILE_BASE_MAX_NUM:usize = 4;
/// 基于第一层 后续层级的 最大总容量增长因子（level-2：10^2 = 100M, level-3：10^3 = 1000M....）
pub const LEVEL_FILE_BASE_GROW_FACTOR:usize = 10;

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
    // mem_table 不可变之后将刷入 level-0 SSTable
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

/// LevelDir抽象
pub struct LevelDir (String, u8);
impl LevelDir {
    pub fn new(level_num: u8) -> Self {
        LevelDir { 0: "level_".to_string(), 1: level_num }
    }

    /// 将 `LevelDir` 装换为 `PathBuf`
    pub fn to_path(&self) -> Result<PathBuf> {
        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        Ok(data_dir.join(format!("{}{}",self.0, self.1)))
    }

    /// 初始化 创建并返回当前 `LevelDir` 的 writer
    pub fn init_level_0_writer(&self) -> Result<BufWriter<File>> {
        let path = self.to_path()?;
        let read_dir_count = read_dir(&path).unwrap();

        let file_count = read_dir_count.count();
        let new_file = || {
            open_option_default(
                get_file_path(&path,
                              gen_sequence(),
                              SERVER_CONFIG.data_file_suffix.as_str()))
        };
        // 没有任何文件
        if file_count == 0 {
            return Ok(BufWriter::new(new_file()?));
        }
        let read_dir = read_dir(&path).unwrap();
        // 已经存在文件
        // 最后一个文件
        let last_file = read_dir.last().unwrap().unwrap();
        if last_file.metadata()?.len() <= LEVEL_0_FILE_MAX_SIZE {
            return Ok(BufWriter::new(
                OpenOptions::new().append(true)
                    .read(true)
                    .write(true)
                    .open(last_file.path())?));
        }
        // 如果文件总数未满
        if file_count < LEVEL_0_FILE_MAX_NUM {
            return Ok(BufWriter::new(new_file()?));
        }
        // 文件总数已满
        else {
            // 到这里的条件已经是 当前的文件总数 == LEVEL_0_FILE_MAX_NUM
            // 并且 最后一个文件不够存放数据了，需要执行major 压缩
            // TODO major;并不是major阻塞，是当前线程主动根据状态阻塞，防止level-0文件数量超过4

            loop {
                if file_count < LEVEL_0_FILE_MAX_NUM { break; }
            }
        }
        // 返回新的
        Ok(BufWriter::new(new_file()?))

    }

}

#[cfg(test)]
mod test {

    #[test]
    fn test() {

    }

}