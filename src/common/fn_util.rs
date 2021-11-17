use crc32fast::Hasher;
use log::LevelFilter;
use std::io::Write;
use std::io;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use anyhow::Result;
use crate::common::error_enum::WiscError;

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

/// 根据 str 返回 SocketAddr
pub fn socket_addr_from_str(content: &str) -> Result<SocketAddr> {
    let addr: Vec<usize> = content.split(&['.',':', '@'][..])
        .map(|ele| {
            ele.parse::<usize>().unwrap()
        }).collect();

    if addr.len() != 5 {
        return Err(anyhow::Error::from(WiscError::SocketAddrParserFail));
    }
    Ok(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(
            *addr.get(0).unwrap() as u8,
            *addr.get(1).unwrap() as u8,
            *addr.get(2).unwrap() as u8,
            *addr.get(3).unwrap() as u8)),
        *addr.get(4).unwrap() as u16))

}