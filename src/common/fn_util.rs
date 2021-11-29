use crc32fast::Hasher;
use log::LevelFilter;
use std::io::{Write, BufWriter};
use std::{io, fs};
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use anyhow::Result;
use crate::common::error_enum::WiscError;
use std::path::{PathBuf, Path};
use std::fs::{OpenOptions, File, create_dir_all};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};

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
        .filter_level(LevelFilter::Info)
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
pub fn sorted_gen_list(path: &Path,file_extension: &str, file_suffix:&str) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> anyhow::Result<_> {
            Ok(res?.path())
        })
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
pub fn get_file_path(dir: &Path, gen: u64, file_suffix: &str) -> PathBuf {
    dir.join(format!("{}{}", gen, file_suffix))
}

/// 根据指定目录获取当前的 file writer
pub fn init_file_writer(path: PathBuf,
            extension: &str,
            suffix: &str,
            file_max_size: usize) -> Result<(BufWriter<File>,AtomicU64)>
{
    create_dir_all(path.clone())?;
    let file_names = sorted_gen_list(path.as_path(),
                                     extension,
                                     suffix)?;
    let file_writer;
    let file_write_name;
    if file_names.is_empty() {
        file_write_name = AtomicU64::new(0);
        file_writer = BufWriter::new(
            open_option_default(get_file_path(path.as_path(),
                                              file_write_name.load(Ordering::SeqCst),
                                              suffix))?
        );

    }else {
        file_write_name = AtomicU64::new(*file_names.last().unwrap());
        let last_file = open_option_default(get_file_path(path.as_path(),
                                                          file_write_name.load(Ordering::SeqCst),
                                                          suffix))?;
        if last_file.metadata()?.len() >= file_max_size as u64 {
            file_write_name.fetch_add(1, Ordering::SeqCst);
            file_writer = BufWriter::new(
                open_option_default(get_file_path(path.as_path(),
                                                  file_write_name.load(Ordering::SeqCst),
                                                  suffix))?
            );

        }else {
            file_writer = BufWriter::new(last_file);
        }
    }
    Ok((file_writer, file_write_name))
}