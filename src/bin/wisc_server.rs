//! 存储引擎服务端

use anyhow::Result;
use log::info;

use r_wisckey::{LogEngine, Server};
use r_wisckey::common::fn_util::{log_init, socket_addr_from_str};
use r_wisckey::config::SERVER_CONFIG;

fn main() {

    // match command.as_str() {
    //     "get" => {
    //         let value =_log_engine.get(key).unwrap();
    //         println!("{:?}",value);
    //     },
    //     "delete" => {
    //         _log_engine.remove(key).unwrap();
    //         println!("delete success :{:?}",key);
    //     },
    //     "insert" => {
    //         let val = maybe_value.expect(USAGE);
    //         match _log_engine.get(key).unwrap() {
    //             Some(_) => {
    //                 eprintln!("insert fail :key: {:?} existed, consider update that",key);
    //             },
    //             None => {
    //                 _log_engine.set(key,val).unwrap();
    //                 println!("insert success :key: {:?},value: {:?}",key,val);
    //             }
    //         }
    //     },
    //     "update" => {
    //         let val = maybe_value.expect(USAGE);
    //         match _log_engine.get(key).unwrap() {
    //             Some(_) => {
    //                 _log_engine.set(key,val).unwrap();
    //                 println!("update success :key: {:?},value: {:?}",key,val);
    //             },
    //             None => {
    //                 eprintln!("insert fail :key: {:?} existed, consider update that",key);
    //             }
    //         }
    //     },
    //     _ => {
    //         eprintln!("{:?}",USAGE);
    //     },
    // }
    log_init();
    run().unwrap();
}

fn run() -> Result<()>{
    let mut server = Server::new(LogEngine::open()?);
    let socket_addr = socket_addr_from_str(SERVER_CONFIG.server_addr.as_str())?;
    server.run(socket_addr)?;
    info!("wisc-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("Listening on {:?}", socket_addr);
    Ok(())
}

