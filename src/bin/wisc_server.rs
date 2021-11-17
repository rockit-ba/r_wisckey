use r_wisckey::{LogEngine, KvsEngine};
use r_wisckey::common::fn_util::log_init;


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



fn main() {
    log_init();
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).expect(USAGE);
    let key = args.get(2).expect(USAGE);
    let maybe_value = args.get(3);
    let mut _log_engine = LogEngine::open().unwrap();

    match command.as_str() {
        "get" => {
            let value =_log_engine.get(key).unwrap();
            println!("{:?}",value);
        },
        "delete" => {
            _log_engine.remove(key).unwrap();
            println!("delete success :{:?}",key);
        },
        "insert" => {
            let val = maybe_value.expect(USAGE);
            match _log_engine.get(key).unwrap() {
                Some(_) => {
                    eprintln!("insert fail :key: {:?} existed, consider update that",key);
                },
                None => {
                    _log_engine.set(key,val).unwrap();
                    println!("insert success :key: {:?},value: {:?}",key,val);
                }
            }
        },
        "update" => {
            let val = maybe_value.expect(USAGE);
            match _log_engine.get(key).unwrap() {
                Some(_) => {
                    _log_engine.set(key,val).unwrap();
                    println!("update success :key: {:?},value: {:?}",key,val);
                },
                None => {
                    eprintln!("insert fail :key: {:?} existed, consider update that",key);
                }
            }
        },
        _ => {
            eprintln!("{:?}",USAGE);
        },
    }

}
