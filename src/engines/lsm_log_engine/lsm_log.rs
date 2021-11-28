//! LSM-tree 存储引擎核心

use anyhow::Result;
use crate::KvsEngine;
use crate::engines::{Scans, init_wal, write_ahead_log};
use std::sync::{Arc, Mutex};
use std::io::BufWriter;
use std::fs::{File, create_dir, create_dir_all};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::config::SERVER_CONFIG;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::path::PathBuf;
use crate::common::fn_util::{sorted_gen_list, open_option_default, get_file_path, init_file_writer};
use crate::engines::base_log_engine::record::CommandType;

#[derive(Debug)]
pub struct LsmLogEngine {
    // 首先接收用户的命令之后需要写 WAL日志，因此
    wal_writer: Arc<Mutex<BufWriter<File>>>,
    wal_write_name: Arc<AtomicU64>,
    // 接着需要写入 MemTable,因为我们需要保持数据的有序性，
    // 因此我们需要特定的数据结构，
    // 注意我们需要两个 mem_table，一个负责写入，写满之后将变为不可变，等待minor compression
    // bool :false 不可变，true :可变
    mem_table: Arc<HashMap<bool,BTreeMap<String,String>>>,
    // mem_table 不可变之后将刷入 SSTable
    sst_writer: Arc<BufWriter<File>>,
    sst_write_name: Arc<AtomicU64>,

}
impl LsmLogEngine {
    pub fn open() -> Result<Self> {
        // 初始化 wal_writer
        let (wal_writer,wal_write_name) = init_wal()?;
        // 初始化 mem_table
        let mut mem_table_group = HashMap::with_capacity(2);
        mem_table_group.insert(true,BTreeMap::<String,String>::new());
        mem_table_group.insert(false,BTreeMap::<String,String>::new());

        // 初始化 sst_writer，从level_0文件夹中查找，因为 minor compression 直接
        // 刷到level_0层级中的sst文件中
        let level_0 = LevelDir::new(0);
        let (sst_writer,sst_write_name) = level_0.init()?;

        Ok(LsmLogEngine {
            wal_writer: Arc::new(Mutex::new(wal_writer)),
            wal_write_name: Arc::new(wal_write_name),
            mem_table: Arc::new(mem_table_vec),
            sst_writer: Arc::new(sst_writer),
            sst_write_name: Arc::new(sst_write_name)
        })
    }

    pub fn get_mut_table(&mut self) -> &mut BTreeMap<String,String> {
        self.mem_table.get_mut(&true).unwrap()
    }
    pub fn get_table(&self) -> &BTreeMap<String,String> {
        self.mem_table.get(&true).unwrap()
    }
}
impl KvsEngine for LsmLogEngine {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // command_type key存在执行set 必定是 update；反之亦然
        let command_type;
        if self.index.get(key).is_some() {
            command_type = CommandType::Update;
        }else {
            command_type = CommandType::Insert;
        }
        // 写 WAL 的逻辑先于其他逻辑，这里失败就会返回用户此次操作失败
        if let Err(err) = write_ahead_log(self.wal_writer.clone(),
                                          &self.wal_write_name,
                                          &command_type,
                                          key,
                                          Some(value)) {
            return Err(err);
        }
        let mut_table = self.get_mut_table();


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