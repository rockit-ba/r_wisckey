//! 服务实现

use crate::client::Command;
use crate::common::error_enum::WiscError;
use crate::KvsEngine;
use anyhow::Result;
use log::{error, info};
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

/// 服务实例
pub struct Server<E: KvsEngine> {
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
            // todo  这里目前只是单线程处理
            loop {
                match &stream {
                    Ok(stream) => {
                        // 处理流
                        if let Err(e) = self.process(stream) {
                            error!("Error on serving client: {:?}", e);
                            break;
                        }
                    }
                    Err(e) => error!("Connection failed: {}", e),
                }
            }
        }
        Ok(())
    }

    // 处理数据流
    fn process(&mut self, tcp: &TcpStream) -> Result<()> {
        let _addr = tcp.peer_addr()?;
        let reader = BufReader::new(tcp);
        let req = bincode::deserialize_from::<BufReader<&TcpStream>, Command>(reader)?;
        info!("接收到请求{:?}", &req);

        let result = client_command_process(&req, &mut self.engine);
        let writer = BufWriter::new(tcp);
        bincode::serialize_into::<BufWriter<&TcpStream>, String>(writer, &result)?;

        Ok(())
    }
}

/// 执行 Command
pub fn client_command_process(command: &Command, engine: &mut dyn KvsEngine) -> String {
    let result = match command {
        Command::Get(key) => match engine.get(key.as_str()) {
            Ok(opt) => {
                format!("{:?}", opt)
            }
            Err(err) => {
                format!("{:?}", err)
            }
        },

        Command::Delete(key) => match engine.remove(key.as_str()) {
            Ok(_) => "OK".to_string(),
            Err(err) => {
                format!("{:?}", err)
            }
        },

        Command::Insert(key, value) => match engine.get(key.as_str()).unwrap() {
            Some(_) => {
                let desc = format!("{:?}", WiscError::KeyExist(key.clone()));
                error!("{:?}", &desc);
                desc
            }
            None => match engine.set(key.as_str(), value.as_str()) {
                Ok(_) => "OK".to_string(),
                Err(err) => {
                    format!("{:?}", err)
                }
            },
        },

        Command::Update(key, value) => match engine.get(key.as_str()).unwrap() {
            Some(_) => match engine.set(key.as_str(), value.as_str()) {
                Ok(_) => "OK".to_string(),
                Err(err) => {
                    format!("{:?}", err)
                }
            },
            None => {
                let desc = format!("{:?}", WiscError::KeyNotExist(key.clone()));
                error!("{:?}", &desc);
                desc
            }
        },
    };
    result
}
