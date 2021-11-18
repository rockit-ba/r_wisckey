//! 存储引擎客户端

use log::{error};
use r_wisckey::config::SERVER_CONFIG;
use r_wisckey::common::fn_util::{log_init, socket_addr_from_str};
use r_wisckey::Client;
use std::process::exit;

fn main() {
    log_init();
    // 默认的 socket_addr
    let socket_addr = socket_addr_from_str(SERVER_CONFIG.server_addr.as_str());
    let addr =  match socket_addr {
        Ok(value) => value,
        Err(err) => {
            error!("{:?}",err);
            exit(1);
        },
    };

    let mut client = Client::connect(addr).unwrap();
    if let Err(err) =  client.run() {
        error!("{:?}",err);
        exit(1);
    }


}