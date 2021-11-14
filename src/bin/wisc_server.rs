use r_wisckey::LogEngine;

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
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).expect(&USAGE);
    let key = args.get(2).expect(&USAGE);
    let maybe_value = args.get(3);
    let mut a = LogEngine::open().expect("unable to open db");
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        let mut a = LogEngine::open().expect("unable to open db");
        println!("{:?}",a);
    }

}