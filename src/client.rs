//! 客户端实例

use std::net::{TcpStream, ToSocketAddrs};
use std::io::{BufReader, BufWriter};
use crate::config::SERVER_CONFIG;

use anyhow::Result;
use rustyline::{Editor};
use rustyline::error::ReadlineError;
use log::{info,warn,error};
use rustyline::validate::{Validator, ValidationContext, ValidationResult};
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};
use crate::client::Command::{Get, Delete, Insert, Update};

const USAGE: &str = "
command parser fail Usage:
    get key
    delete key
    insert key value
    update key value
";

/// command line 前缀
const LINE_PREFIX:&str = "wisc-db>> ";

#[allow(dead_code)]
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,

    editor: Editor<InputValidator>,
}
impl Client {
    /// 获取服务端连接实例
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(Client {
            reader: BufReader::new(tcp_reader),
            writer: BufWriter::new(tcp_writer),
            editor: Editor::<InputValidator>::new(),
        })
    }

    /// 启动
    pub fn run(&mut self) -> Result<()> {
        if self.editor.load_history(SERVER_CONFIG.command_history.as_str()).is_err() {
            info!("No previous history.");
        }
        self.editor.set_helper(Some(InputValidator));
        loop {
            let readline = self.editor.readline(LINE_PREFIX);
            match readline {
                Ok(line) => {
                    self.editor.add_history_entry(line.as_str());
                    let command = command_parser(line.as_str()).unwrap();
                    info!("command: {:?}", command);
                },
                Err(ReadlineError::Interrupted) => {
                    info!("CTRL-C");
                    break
                },
                Err(ReadlineError::Eof) => {
                    info!("CTRL-D");
                    break
                },
                Err(err) => {
                    error!("Error: {:?}", err);
                    break
                }
            }
        }
        self.editor.save_history(SERVER_CONFIG.command_history.as_str())?;
        Ok(())
    }

}

/// 客户端命令解析
///
/// insert key value
fn command_parser(command: &str) -> Option<Command> {
    let arr:Vec<String> = command.trim()
        .replace(";","")
        .replace("\n"," ")
        .split_whitespace()
        .map(|ele|ele.to_string())
        .collect();
    return match arr.len() {
        // get key
        // delete key
        2 => {
            let key = arr.get(1).unwrap();
            match arr.get(0).unwrap().as_str() {
                "get" => {
                    Some(Get(key.to_string()))
                },
                "delete" => {
                    Some(Delete(key.to_string()))
                },
                _ => {
                    None
                }
            }
        },
        // insert key value
        // update key value
        3 =>{
            let key = arr.get(1).unwrap();
            let value = arr.get(2).unwrap();
            match arr.get(0).unwrap().as_str() {
                "insert" => {
                    Some(Insert(key.to_string(),value.to_string()))
                },
                "update" => {
                    Some(Update(key.to_string(),value.to_string()))
                },
                _ => {
                    None
                }
            }
        },
        _ => {
            None
        }
    }

}

#[derive(Debug)]
pub enum Command {
    Get(String),
    Delete(String),
    Insert(String,String),
    Update(String,String),
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator;

impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        if !input.ends_with(';') {
            warn!("命令 [{:?}] 不完整,尝试以 ';' 结尾",input);
            return Ok(ValidationResult::Incomplete);
        }
        if command_parser(input).is_none() {
            return Ok(ValidationResult::Invalid(Some(USAGE.to_string())));
        }
        Ok(ValidationResult::Valid(None))
    }

    fn validate_while_typing(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::client::command_parser;

    #[test]
    fn test() {
        let aa = command_parser("get\naa");
        println!("{:?}",aa);
    }
}
