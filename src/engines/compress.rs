//! 压缩处理逻辑

use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use crate::config::SERVER_CONFIG;
use std::thread::sleep;
use std::time::Duration;
use std::sync::Arc;

const COMPRESS_THREAD:&str = "compress_thread";

/// 定时检查 compress_counter 并执行压缩
pub fn compress(compress_counter: Arc<AtomicUsize>) -> Result<()> {
    thread::Builder::new().name(COMPRESS_THREAD.to_string()).spawn(move || {
        loop {
            // todo
            if compress_counter.load(Ordering::SeqCst) >= SERVER_CONFIG.compress_threshold {
                println!("compress....");

            }else {
                println!("don't need compress....");
            }
            sleep(Duration::from_secs(SERVER_CONFIG.compress_interval));
        }
    })?;

    Ok(())
}



