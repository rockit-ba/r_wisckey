//! 存储引擎服务端

use anyhow::Result;
use log::{info,error};

use r_wisckey::{LsmLogEngine, Server};
use r_wisckey::common::fn_util::{log_init, socket_addr_from_str};
use r_wisckey::config::SERVER_CONFIG;
use std::process::exit;

const BANNER:&str = r#"                  .__                  __
_______  __  _  __|__|  ______  ____  |  | __  ____  ___.__.
\_  __ \ \ \/ \/ /|  | /  ___/_/ ___\ |  |/ /_/ __ \<   |  |
 |  | \/  \     / |  | \___ \ \  \___ |    < \  ___/ \___  |
 |__|______\/\_/  |__|/____  > \___  >|__|_ \ \___  >/ ____|
    /_____/                \/      \/      \/     \/ \/     "#;

fn main() {
    log_init();
    if let Err(err) = run() {
        error!("{:?}",err);
        exit(1);
    }
}

fn run() -> Result<()>{
    let mut engine = LsmLogEngine::open()?;

    let mut server = Server::new(engine);
    let socket_addr = socket_addr_from_str(SERVER_CONFIG.server_addr.as_str())?;
    info!("{}",BANNER);
    info!("wisc-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("Listening on {:?}", &socket_addr);

    server.run(socket_addr)?;
    Ok(())
}

