use crc32fast::Hasher;
use log::LevelFilter;
use std::io::Write;
use std::io;

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

/// 是否是 `UnexpectedEof` 错误
///
/// 根据全局的anyhow::Error 判断
pub fn is_eof_err(err: &anyhow::Error) -> bool {
    let may_err = err.root_cause().downcast_ref::<bincode::Error>();
    if let Some(box_error) = may_err {
        match &**box_error {
            bincode::ErrorKind::Io(_err) => {
                match _err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        true
                    },
                    _ => false
                }
            },
            _ => false,
        }
    } else { false }
}