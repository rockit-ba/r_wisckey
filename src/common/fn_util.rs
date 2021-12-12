use anyhow::Result;
use chrono::Local;
use crc32fast::Hasher;
use lazy_static::lazy_static;
use log::LevelFilter;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::{fs, io};

use crate::common::error_enum::WiscError;

/// 日志格式初始化
pub fn log_init() {
    env_logger::builder()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] [{}] {}: {}",
                record.line().unwrap(),
                record.target(),
                record.level(),
                record.args()
            )
        })
        .filter_level(LevelFilter::Info)
        .init();
}

/// 根据字节序列获取 u32 checksum 值
pub fn checksum(content: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(content);
    hasher.finalize()
}

/// check sum 校验
pub fn checksum_verify(content: &[u8], old_checksum: u32) -> bool {
    checksum(content) == old_checksum
}

/// 是否是 `UnexpectedEof` 错误
///
/// 根据全局的anyhow::Error 判断
pub fn is_eof_err(err: &anyhow::Error) -> bool {
    let may_err = err.root_cause().downcast_ref::<bincode::Error>();
    if let Some(box_error) = may_err {
        match &**box_error {
            bincode::ErrorKind::Io(_err) => matches!(_err.kind(), io::ErrorKind::UnexpectedEof),
            _ => false,
        }
    } else {
        false
    }
}

/// 根据 str 返回 SocketAddr
pub fn socket_addr_from_str(content: &str) -> Result<SocketAddr> {
    let addr: Vec<usize> = content
        .split(&['.', ':', '@'][..])
        .map(|ele| ele.parse::<usize>().unwrap())
        .collect();

    if addr.len() != 5 {
        return Err(anyhow::Error::from(WiscError::SocketAddrParserFail));
    }
    Ok(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(
            *addr.get(0).unwrap() as u8,
            *addr.get(1).unwrap() as u8,
            *addr.get(2).unwrap() as u8,
            *addr.get(3).unwrap() as u8,
        )),
        *addr.get(4).unwrap() as u16,
    ))
}

/// 默认的文件句柄option
pub fn open_option_default(path: PathBuf) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .create(true)
        .write(true)
        .append(true)
        .open(path)?)
}

///  排序数据目录下的所有的数据文件，获取文件名集合
pub fn sorted_gen_list(path: &Path, file_extension: &str, file_suffix: &str) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> anyhow::Result<_> { Ok(res?.path()) })
        // 过滤属于文件的path，并且文件扩展名是 file_suffix
        .filter(|path| path.is_file() && path.extension() == Some(OsStr::new(file_extension)))
        // 过滤出需要的 path
        .flat_map(|path| {
            // 获取文件名
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(file_suffix))
                .map(str::parse::<u64>)
        })
        // 扁平化，相当于去除flat 包装，取得里面的 u64 集合
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// 根据数据目录和文件编号获取指定的文件地址
pub fn get_file_path(dir: &Path, gen: i64, file_suffix: &str) -> PathBuf {
    dir.join(format!("{}{}", gen, file_suffix))
}

lazy_static! {
    /// 全局自增元素(并非始终全局+1自增，重启后将重置基础值，从时序上来看，它是递增的)
    pub static ref SEQUENCE:AtomicI64 = {
        AtomicI64::new(Local::now().timestamp_millis())
    };
}

/// 获取全局增长 `i64` 序列
pub fn gen_sequence() -> i64 {
    SEQUENCE.fetch_add(1, Ordering::SeqCst)
}
