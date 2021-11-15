use crc32fast::Hasher;
use log::LevelFilter;
use std::io::Write;

/// 根据字节序列获取 u32 checksum 值
pub fn checksum(content: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(content);
    hasher.finalize()
}

/// 日志格式初始化
pub fn log_init() {
    env_logger::builder()
        .format(|buf, record| {
            writeln!(buf, "[{}] [{}] {}: {}",
                     record.line().unwrap(),
                     record.target(),
                     record.level(),
                     record.args())
        })
        .filter_level(LevelFilter::Debug)
        .init();
}