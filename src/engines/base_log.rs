//! 日志存储实现 引擎核心

use std::fs::{File, OpenOptions};
use std::collections::btree_map::BTreeMap;
use std::{env, fs, io};
use std::path::{PathBuf, Path};
use std::ffi::OsStr;
use std::io::{BufReader, Read, BufWriter, Write,};
use std::collections::HashMap;

use crate::common::{ByteBuf, checksum};
use crate::{WiscError, KvsEngine};
use crate::engines::record::{RECORD_HEADER_SIZE, RecordHeader, KVPair};
use crate::engines::Scans;

use anyhow::Result;
use log::info;

/// 存放数据文件的基础目录
const DATA_DIR:&str = "data";
/// 数据文件的后缀名
const DATA_FILE_SUFFIX:&str = ".wisc";
/// 数据文件的扩展名
const DATA_FILE_EXTENSION:&str = "wisc";
/// 数据文件最大容量，超出则创建新文件写入数据
const FILE_MAX_SIZE: u64 = 1024*1024*1;

/// 存储引擎
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
            let mut reader = BufReader::new(
                File::open(get_log_path(&data_dir.as_path(), name))?
            );
            // 加载log文件到index中，在这个过程中不断执行insert 和remove操作，根据set 或者 rm
            load(&mut reader, &mut index)?;
            // 一个log 文件对应一个  bufreader
            readers.insert(name, reader);
        }
        let curr_log = log_names.last();
        let open_option = |path: PathBuf| {
            OpenOptions::new()
                .read(true)
                .create(true)
                .write(true)
                .append(true)
                .open(path)
        };

        // writer 初始化
        let writer = match curr_log {
            // 如果存在最后写入数据的文件
            Some(&name) => {
                let file = open_option(get_log_path(&data_dir,name))?;
                if file.metadata()?.len() >= FILE_MAX_SIZE {
                    // 如果文件大小超过规定大小，则创建新文件
                    BufWriter::new(open_option(get_log_path(&data_dir,name + 1))?)
                }else {
                    BufWriter::new(file)
                }
            },
            None => {
                // 如果不存在任何一个文件，则创建初始的文件
                BufWriter::new(open_option(get_log_path(&data_dir,0))?)
            }
        };

        Ok(LogEngine {
            readers,
            writer,
            index
        })

    }

    // 往文件中添加 操作数据
    fn append(&mut self, command_type: u8, kv: &KVPair) -> Result<()> {
        let header = RecordHeader::new(command_type,
                                       checksum(data_byte.as_slice()),
                                       data_byte.len() as u32);

        let mut header_byte = bincode::serialize(&header)?;
        let mut data_byte = bincode::serialize(kv)?;
        info!("append header:{:?}",&header);
        info!("append data:{:?}",kv);
        /*
         将header_byte 和 data_byte进行合并，并且write，注意顺序不能替换
         因为我们首先是读取定长的 header
         */
        header_byte.append(&mut data_byte);
        self.writer.write_all(header_byte.as_slice())?;
        Ok(())
    }
}
impl KvsEngine for LogEngine {
    fn set(&mut self, key: &String, value: &String) -> Result<()> {
        // 放入内存中
        self.index.insert(key.to_string(),value.to_string());
        let kv = KVPair::new(key.to_string(),value.to_string());
        // 持久化log 文件
        self.append(1_u8,&kv)?;
        Ok(())
    }

    fn get(&self, key: &String) -> Result<Option<String>> {
        // 从内存中获取
        Ok(self.index.get(key).cloned())
    }

    fn scan(&self, range: Scans) -> Result<Option<Vec<String>>> {
        todo!()
    }

    fn remove(&mut self, key: &String) -> Result<()> {
        todo!()
    }
}

///  排序数据目录下的所有的数据文件，获取文件名集合
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> anyhow::Result<_> {
            Ok(res?.path())
        })
        // 过滤属于文件的path，并且文件扩展名是 wisc
        .filter(|path| path.is_file() && path.extension() == Some(DATA_FILE_EXTENSION.as_ref()))
        // 过滤出需要的 path
        .flat_map(|path| {
            // 获取文件名
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(DATA_FILE_SUFFIX))
                .map(str::parse::<u64>)
        })
        // 扁平化，相当于去除flat 包装，取得里面的 u64 集合
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// 根据数据目录和文件编号获取指定的文件地址
fn get_log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}{}", gen, DATA_FILE_SUFFIX))
}

/// 加载单个文件中的单个record
fn process_record<R: Read >(reader: &mut R) -> Result<KVPair> {
    let mut header_buf = ByteBuf::with_capacity(RECORD_HEADER_SIZE);
    {
        reader.by_ref().take(RECORD_HEADER_SIZE as u64).read_to_end(&mut header_buf)?;
    }
    // 得到record header
    let header = bincode::deserialize::<RecordHeader>(header_buf.as_slice())?;
    info!("load header:{:?}",&header);
    let saved_checksum = header.checksum;

    let data_len = header.data_len;
    // data 字节数据
    let mut data_buf = ByteBuf::with_capacity(data_len as usize);
    {
        reader.by_ref().take(data_len as u64).read_to_end(&mut data_buf)?;
    }

    let checksum = checksum(data_buf.as_slice());
    if checksum != saved_checksum {
        // 数据损坏
        return Err(anyhow::Error::from(
            WiscError::DataCorruption { checksum, saved_checksum }
        ))
    }
    let kv = bincode::deserialize::<KVPair>(data_buf.as_slice())?;
    info!("load data:{:?}",&kv);
    Ok(kv)
}

/// 循环加载单个文件中的record
fn load(
    reader: &mut BufReader<File>,
    index: &mut BTreeMap<String,String>,
) -> Result<()> {
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
        // 将数据放入内存
        index.insert(
            kv.key,
            kv.value,
        );
    }
    Ok(())
}

