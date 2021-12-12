//! 数据文件存储

#![allow(dead_code)]

use anyhow::Result;
use std::env;
use std::fs::{create_dir_all, read_dir, File, OpenOptions};
use std::io::BufWriter;
use std::path::PathBuf;

use crate::common::fn_util::{gen_sequence, get_file_path, open_option_default};
use crate::config::SERVER_CONFIG;

/// LEVEL_0 单个文件的大小 1M
pub const LEVEL_0_FILE_MAX_SIZE: u64 = 1024 * 1024;
/// LEVEL_0 层所有文件的最大数量
pub const LEVEL_0_FILE_MAX_NUM: usize = 4;

/// 非 LEVEL_0 单个文件的大小 2M
pub const LEVEL_FILE_MAX_SIZE: usize = 1024 * 1024 * 2;
/// 换句话说就是level-1层的所有文件最大个数，也就是level-1总大小在 10M
pub const LEVEL_FILE_BASE_MAX_NUM: usize = 4;
/// 基于第一层 后续层级的 最大总容量增长因子（level-2：10^2 = 100M, level-3：10^3 = 1000M....）
pub const LEVEL_FILE_BASE_GROW_FACTOR: usize = 10;

/// LevelDir抽象
pub struct LevelDir(String, u8);
impl LevelDir {
    pub fn new(level_num: u8) -> Self {
        LevelDir {
            0: "level_".to_string(),
            1: level_num,
        }
    }

    /// 将 `LevelDir` 装换为 `PathBuf`
    pub fn to_path(&self) -> Result<PathBuf> {
        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        let level_dir = data_dir.join(format!("{}{}", self.0, self.1));
        create_dir_all(&level_dir)?;
        Ok(level_dir)
    }

    /// 初始化 创建并返回当前 `LevelDir` 的 writer
    pub fn init_level_0_writer(&self) -> Result<BufWriter<File>> {
        let path = self.to_path()?;
        let read_dir_count = read_dir(&path)?;

        let file_count = read_dir_count.count();
        let new_file = || {
            open_option_default(get_file_path(
                &path,
                gen_sequence(),
                SERVER_CONFIG.data_file_suffix.as_str(),
            ))
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
                OpenOptions::new()
                    .append(true)
                    .read(true)
                    .write(true)
                    .open(last_file.path())?,
            ));
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
                if file_count < LEVEL_0_FILE_MAX_NUM {
                    break;
                }
            }
        }
        // 返回新的
        Ok(BufWriter::new(new_file()?))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        let level = LevelDir::new(0);
        level.to_path().unwrap();
    }
}
