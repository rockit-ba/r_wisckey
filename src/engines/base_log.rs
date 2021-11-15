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


const DATA_DIR:&str = "data";
const DATA_FILE_SUFFIX:&str = ".wisc";
const DATA_FILE_EXTENSION:&str = "wisc";
const FILE_MAX_SIZE: u64 = 1024*1024*1;

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
                let file = open_option(get_log_path(&data_dir,name))?;
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

    // 往文件中添加 操作数据
    fn append(&mut self, command_type: u8, kv: &KVPair) -> Result<()> {
        let mut data_byte = bincode::serialize(kv)?;

        let header = RecordHeader::new(command_type,
                                       checksum(data_byte.as_slice()),
                                       data_byte.len() as u32);

        let mut header_byte = bincode::serialize(&header)?;

        info!("append header:{:?}",&header);
        info!("append data:{:?}",kv);
        header_byte.append(&mut data_byte);
        self.writer.write_all(header_byte.as_slice())?;
        Ok(())
    }
}

impl KvsEngine for LogEngine {
    fn set(&mut self, key: &String, value: &String) -> Result<()> {
        self.index.insert(key.to_string(),value.to_string());
        let kv = KVPair::new(key.to_string(),value.to_string());
        self.append(1_u8,&kv)?;
        Ok(())
    }

    fn get(&self, key: &String) -> Result<Option<String>> {
        Ok(self.index.get(key).cloned())
    }

    fn scan(&self, range: Scans) -> Result<Option<Vec<String>>> {
        todo!()
    }

    fn remove(&mut self, key: &String) -> Result<()> {
        todo!()
    }
}

///  排序数据目录下的所有的数据文件
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
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

    // 获取数据 len
    let data_len = header.data_len;
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

/// 加载单个文件中的record
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
        index.insert(
            kv.key,
            kv.value,
        );
    }
    Ok(())
}

