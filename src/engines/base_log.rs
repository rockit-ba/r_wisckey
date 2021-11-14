//! 日志存储实现 引擎核心

use std::fs::{File, OpenOptions};
use std::collections::btree_map::BTreeMap;
use std::{env, fs, io};

use crate::WiscError;
use std::path::{PathBuf, Path};
use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, BufWriter, SeekFrom};
use std::collections::HashMap;
use crc::{Crc, CRC_32_ISCSI};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_derive::{Serialize,Deserialize};
use anyhow::private::kind::AdhocKind;
use crate::engines::record::{RECORD_HEADER_SIZE, RecordHeader};
use std::error::Error;
use std::borrow::Borrow;
use std::sync::BarrierWaitResult;


const DATA_DIR:&str = "data";
const DATA_FILE_SUFFIX:&str = ".wisc";
const DATA_FILE_EXTENSION:&str = "wisc";
const FILE_MAX_SIZE: u64 = 1024*1024*10;

pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct LogEngine {
    readers: HashMap<u64,BufReader<File>>,
    writer: BufWriter<File>,
    index: BTreeMap<String,String>,
}

impl LogEngine {
    // 从指定的数据目录打开一个 LogEngine
    pub fn open() -> anyhow::Result<Self> {
        let data_dir = env::current_dir()?.join(DATA_DIR);
        fs::create_dir_all(&data_dir)?;

        let mut readers = HashMap::<u64,BufReader<File>>::new();
        let mut index = BTreeMap::<String,String>::new();

        let log_names = sorted_gen_list(&data_dir.as_path())?;
        log::info!("{:?}",&log_names);
        for &name in &log_names {
            // reader 和 writer 中的 file seek 都是 current 模式
            let mut reader = BufReader::new(
                File::open(get_log_path(&data_dir.as_path(), name))?
            );
            // 加载log文件到index中，在这个过程中不断执行insert 和remove操作，根据set 或者 rm
            load(&mut reader, &mut index)?;
            // 一个log 文件对应一个  BufReaderWithPos<File>
            readers.insert(name, reader);
        }
        let curr_log = log_names.last();
        let open_option = |path: PathBuf| {
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)
        };
        let writer = match curr_log {
            Some(&name) => {
                let mut file = open_option(get_log_path(&data_dir,name))?;
                if file.metadata()?.len() >= FILE_MAX_SIZE {
                    BufWriter::new(open_option(get_log_path(&data_dir,name + 1))?)
                }else {
                    BufWriter::new(file)
                }
            },
            None => {
                BufWriter::new(open_option(get_log_path(&data_dir,0))?)
            }
        };

        Ok(LogEngine {
            readers,
            writer,
            index
        })

    }

}

fn sorted_gen_list(path: &Path) -> anyhow::Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        // map：map方法返回的是一个object，map将流中的当前元素替换为此返回值；
        // flatMap：flatMap方法返回的是一个stream，flatMap将流中的当前元素替换为此返回流拆解的流元素；
        // 获取path
        .flat_map(|res| -> anyhow::Result<_> {
            Ok(res?.path())
        })
        // 过滤属于文件的path，并且文件扩展名是 wisc
        .filter(|path| path.is_file() && path.extension() == Some(DATA_FILE_EXTENSION.as_ref()))
        // 过滤出需要的 path
        .flat_map(|path| {
            // 获取文件名
            path.file_name()
                // OsStr 转换为 str 类型
                .and_then(OsStr::to_str)
                // 去除后缀名
                .map(|s| s.trim_end_matches(DATA_FILE_SUFFIX))
                // 将u64 str 转换为 u64
                .map(str::parse::<u64>)
        })
        // 扁平化，相当于去除flat 包装，取得里面的 u64 集合
        .flatten()
        .collect();
    // 对结果进行排序
    gen_list.sort_unstable();
    Ok(gen_list)
}

fn get_log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}{}", gen, DATA_FILE_SUFFIX))
}

fn process_record<R: Read>(reader: &mut R) -> anyhow::Result<KeyValuePair> {
    let mut data = Vec::<u8>::with_capacity(RECORD_HEADER_SIZE);

    {
        reader.by_ref().take(RECORD_HEADER_SIZE as u64).read_to_end(&mut data)?;
    }
    let header:RecordHeader = bincode::deserialize(data.as_slice())?;
    log::info!("{:?}",data);
    let saved_checksum = reader.read_u32::<LittleEndian>()?;
    let key_len = reader.read_u32::<LittleEndian>()?;
    let val_len = reader.read_u32::<LittleEndian>()?;

    let data_len = key_len + val_len;
    let mut data = Vec::<u8>::with_capacity(data_len as usize);
    {
        reader.by_ref().take(data_len as u64).read_to_end(&mut data)?;
    }

    let checksum = CASTAGNOLI.checksum(data.as_slice());
    if checksum != saved_checksum {
        // 数据损坏
        panic!(
            "data corruption encountered ({:08x} != {:08x})",
            checksum, saved_checksum
        );
    }
    let value = data.split_off(key_len as usize);
    let key = data;
    Ok(KeyValuePair {
        key,
        value
    })
}


fn load(
    reader: &mut BufReader<File>,
    index: &mut BTreeMap<String,String>,
) -> anyhow::Result<()> {
    loop {
        let recorde = process_record(reader);
        let kv = match recorde {
            Ok(kv) => kv,
            Err(err) => {
                let may_err = err.root_cause().downcast_ref::<bincode::Error>();
                return match may_err {
                    Some(may_err) => {
                        if may_err.to_string().contains(
                            io::Error::from(io::ErrorKind::UnexpectedEof).to_string().as_str()) {
                            break;
                        }
                        Err(anyhow::Error::from(err))
                    }
                    None => Err(anyhow::Error::from(err)),
                }
            }
        };
        index.insert(
            String::from_utf8(kv.key)?,
            String::from_utf8(kv.value)?,
        );
    }
    Ok(())
}

