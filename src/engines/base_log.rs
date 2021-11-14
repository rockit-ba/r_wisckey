//! 并发说明：
//!
//! 1 理解和维护顺序一致性：比如我们的 writer ，写文件和写索引的二者执行顺序需要维护
//! 任何必须在另一个之前或之后发生的操作,
//! 都必须明确地安排为使用同步类型或操作来执行，
//! 无论它们是锁、原子或其他。
//!
//! 2 识别 不可变的value：
//! 不可变值都是 Sync的。
//! 不可变值最适合并发——只需将它们抛在 Arc 后面，不要再考虑它们。
//! 在下面中，我们识别 PathBuf 是 immutable.
//!
//! 3 重复（clone）value而不是共享它：
//! 克隆有时在 Rust 中名声不佳，尤其是具有任意大小的克隆类型，例如 String 和 Vec。
//! 但是克隆通常是完全合理的：在某些情况下很难避免克隆，而且 CPU 非常擅长复制内存缓冲区。
//! 此外，考虑到我们的用例，支持服务器所需的状态副本数量受线程池中线程数量的限制。
//! PathBuf is easily clonable.
//!
//! 4 按角色分解数据结构
//! 在我们的用例中，我们有两个明确的角色：reader和writer（可能还有第三个用于压缩器）。
//! 将reader和writer逻辑分离成他们自己的并发类型在 Rust 中很常见。
//! reader有自己的数据集可以使用，而writer有自己的数据集，这为封装提供了一个很好的机会，
//! 所有的读操作属于一种类型，所有的写操作属于另一种类型。
//! 进行这种区分将进一步使两者都访问的资源变得非常容易识别，
//! 因为reader和writer都需要这些资源的共享句柄。
//!
//! 5 使用专门的并发数据结构
//! 仅仅知道有哪些工具可用以及在哪些场景下使用它们可能是并行编程中最困难的部分。
//! 在这个项目中，由于内存索引是某种类型的关联数据结构（又名“map”），如树或哈希表，
//! 很自然地会询问是否存在这种并发数据结构：当然有了。https://libs.rs/
//!
//! 6 推迟清理
//! 像克隆一样，垃圾收集在 Rust 中经常被人反对——避免 GC 几乎是 Rust 存在的全部原因。
//! 但众所周知，垃圾收集是不可避免的，
//! “垃圾收集”和“内存回收”实际上是同义词，每种语言都混合使用垃圾收集策略。
//! java 中所有的内存不一定都是GC回收的，高性能下会开发者会重用内存且批量释放。
//! 同样，在 Rust 中，并非所有内存都被确定性地释放。
//! 简单的例子是实现 [资源计数] 的 Rc 和 Arc 类型，这是一种简单的 GC。
//! 全局垃圾收集器的最大好处之一是它们使许多无锁数据结构成为可能。学术文献中描述的许多无锁数据结构都依赖于 GC 进行操作。
//! 垃圾收集有多种形式，其将资源清理延迟到未来某个时间的基本策略在许多场景中都很强大。
//!
//! 7 用原子类共享标记和计数器
//!
//! 在幕后，大多数并发数据结构是使用原子操作或“atomics”实现的。Atomics 在单个内存单元上运行，通常在 8 到 128 字节之间，
//! 通常是字大小（与指针相同的字节数，以及 Rust 使用类型）。如果两个线程正确使用原子，
//! 那么在一个线程中写入的结果对另一个线程中的读取立即可见。除了使读取或写入立即可见之外，
//! 原子操作还通过 Ordering 标志限制编译器和 CPU 在 Rust 中重新排序指令的方式。
//! 当从 锁的粗粒度并行 转向 更细粒度的并行时，通常需要用原子来增强现成的并发数据结构。

use std::fs::{File, OpenOptions};
use std::collections::btree_map::BTreeMap;
use std::{env, fs, io};

use crate::{Result,WiscError};
use std::path::{PathBuf, Path};
use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, BufWriter, SeekFrom};
use std::collections::HashMap;
use crc::{Crc, CRC_32_ISCSI};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_derive::{Serialize,Deserialize};
use failure::{AsFail, Fail};


const DATA_DIR:&str = "data";
const DATA_FILE_SUFFIX:&str = ".wisc";
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
    pub fn open() -> Result<Self> {
        let data_dir = env::current_dir()?.join(DATA_DIR);
        fs::create_dir_all(&data_dir)?;

        let mut readers = HashMap::<u64,BufReader<File>>::new();
        let mut index = BTreeMap::<String,String>::new();

        let log_names = sorted_gen_list(&data_dir.as_path())?;

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
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        // map：map方法返回的是一个object，map将流中的当前元素替换为此返回值；
        // flatMap：flatMap方法返回的是一个stream，flatMap将流中的当前元素替换为此返回流拆解的流元素；
        // 获取path
        .flat_map(|res| -> Result<_> {
            Ok(res?.path())
        })
        // 过滤属于文件的path，并且文件扩展名是 log
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
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

fn load(
    reader: &mut BufReader<File>,
    index: &mut BTreeMap<String,String>,
) -> Result<()> {
    loop {
        let recorde = process_record(reader);
        let kv = match recorde {
            Ok(kv) => kv,
            Err(err) => {
                match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(WiscError::from(err)),
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

fn process_record<R: Read>(reader: &mut R) -> io::Result<KeyValuePair> {
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

