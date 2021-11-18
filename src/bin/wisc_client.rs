//! 存储引擎客户端

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use log::error;
use r_wisckey::config::SERVER_CONFIG;
use r_wisckey::common::fn_util::{log_init, socket_addr_from_str};
use r_wisckey::Client;
use std::process::exit;

#[allow(unused)]
fn main() {
    log_init();
    let socket_addr = socket_addr_from_str(SERVER_CONFIG.server_addr.as_str());
    let addr =  match socket_addr {
        Ok(value) => value,
        Err(err) => {
            error!("{:?}",err);
            exit(1);
        },
    };

    let opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("host")
                .short("h")
                .long("host")
                .help("Host connect to")
                .takes_value(true)
                .required(true)
                .default_value(addr.ip().to_string().as_str()),
        )
        .arg(
            clap::Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Port number to connect to")
                .takes_value(true)
                .required(true)
                .default_value(addr.port().to_string().as_str()),
        )
        .get_matches();

    let mut client = Client::connect(addr).unwrap();

    if let Err(err) =  client.run() {
        error!("{:?}",err);
        exit(1);
    }

}