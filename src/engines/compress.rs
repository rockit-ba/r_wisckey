//! 压缩处理逻辑

use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use std::{thread, env};

use anyhow::Result;
use log::info;
use crate::config::SERVER_CONFIG;
use std::thread::sleep;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::io::BufWriter;
use std::fs::File;
use crate::engines::base_log::{open_option, get_log_path};

/// 压缩任务的线程 `name`
const COMPRESS_THREAD:&str = "compress_thread";

/// 定时检查 compress_counter 并执行压缩
pub fn compress(write_name: Arc<AtomicU64>,
                writer: Arc<Mutex<BufWriter<File>>>,
                compress_counter: Arc<AtomicUsize>) -> Result<()> {
    thread::Builder::new().name(COMPRESS_THREAD.to_string()).spawn(move || -> Result<()> {
        let data_dir = env::current_dir()?.join(&SERVER_CONFIG.data_dir);
        loop {
            // todo
            if compress_counter.load(Ordering::SeqCst) >= SERVER_CONFIG.compress_threshold {
                info!("compress....");


                write_name.fetch_add(1_u64,Ordering::SeqCst);
                let gen = write_name.load(Ordering::SeqCst);
                // 新建文件并将 engine writer 指向该新文件
                *writer.lock().unwrap() =
                    BufWriter::new(open_option(get_log_path(&data_dir,gen))?);



                info!("compress end ...");

            };
            sleep(Duration::from_secs(SERVER_CONFIG.compress_interval));
        }
    })?;

    Ok(())
}



