use r_wisckey::LogEngine;
use log::LevelFilter;
use log::info;
use std::io::Write;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    wisc_server.exe get key
    wisc_server.exe delete key
    wisc_server.exe insert key value
    wisc_server.exe update key value
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    wisc_server get key
    wisc_server delete key
    wisc_server insert key value
    wisc_server update key value
";

/// 日志格式初始化
pub fn log_init() {
    env_logger::builder()
        .format(|buf, record| {
            writeln!(buf, "[{}] [{}] {}: {}",
                     record.line().unwrap(),
                     record.target(),
                     record.level(),
                     record.args())
        })
        .filter_level(LevelFilter::Debug)
        .init();
}

fn main() {
    log_init();
    // let args: Vec<String> = std::env::args().collect();
    // let command = args.get(1).expect(&USAGE);
    // let key = args.get(2).expect(&USAGE);
    // let maybe_value = args.get(3);
    let mut _log_engine = LogEngine::open().unwrap();

}

#[cfg(test)]
mod test {
    use super::*;
    use r_wisckey::KvsEngine;

    #[test]
    fn test_get() {
        log_init();
        let mut log_engine = LogEngine::open().unwrap();
        let value_01 = log_engine.get(&String::from("鸢一折纸")).unwrap();
        let value_02 = log_engine.get(&String::from("十香")).unwrap();
        info!("{:?}",value_01);
        info!("{:?}",value_02);
    }
    #[test]
    fn test_set() {
        log_init();
        let mut log_engine = LogEngine::open().unwrap();
        log_engine.set(&String::from("鸢一折纸"),&String::from("约会大作战"));
        log_engine.set(&String::from("十香"),&String::from("约会大作战"));
        log_engine.set(&String::from("鸢一折纸"),&String::from("春物"));

    }


}