//! 服务实现

use log::{info,error};
use anyhow::Result;
use std::net::{ToSocketAddrs, TcpListener, TcpStream};
use std::io::{BufReader, BufWriter, Write};
use crate::KvsEngine;

/// 服务实例
pub struct Server<E: KvsEngine> {
    #[allow(dead_code)]
    engine: E,
}
impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E) -> Self {
        Server { engine }
    }

    /// 在给定的地址上启动server 监听
    pub fn run<S: ToSocketAddrs>(&mut self, addr: S) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // 处理流
                    if let Err(e) = self.process(stream) {
                        error!("Error on serving client: {:?}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    // 处理数据流
    fn process(&mut self, tcp: TcpStream) -> Result<()> {
        let _addr = tcp.peer_addr()?;
        let reader = BufReader::new(&tcp);
        let req = bincode::deserialize::<String>(reader.buffer())?;
        info!("接收到请求{:?}",&req);
        let mut writer = BufWriter::new(&tcp);
        writer.write_all("response data".as_bytes())?;
        writer.flush()?;
        Ok(())
    }
}