//! 客户端实例

use crate::config::SERVER_CONFIG;
use std::io::{BufReader, BufWriter};
use std::net::{TcpStream, ToSocketAddrs};

use crate::client::Command::{Delete, Get, Insert, Update};
use anyhow::Result;
use log::{error, info, warn};
use rustyline::error::ReadlineError;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::Editor;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};
use serde_derive::{Deserialize, Serialize};

/// helper
const USAGE: &str = "
command parser fail, Usage:
    get key;
    delete key;
    insert key value;
    update key value;
";

/// command line 前缀
const LINE_PREFIX: &str = "wisc-db>> ";

const GET: &str = "get";
const DELETE: &str = "delete";
const INSERT: &str = "insert";
const UPDATE: &str = "update";

/// 客户端实体
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    editor: Editor<InputValidator>,
}
impl Client {
    /// 获取服务端连接实例
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        info!("Success connection to {:?}", tcp_reader.peer_addr()?);

        let tcp_writer = tcp_reader.try_clone()?;
        Ok(Client {
            reader: BufReader::new(tcp_reader),
            writer: BufWriter::new(tcp_writer),
            editor: Editor::<InputValidator>::new(),
        })
    }

    /// 启动
    pub fn run(&mut self) -> Result<()> {
        if self
            .editor
            .load_history(SERVER_CONFIG.command_history.as_str())
            .is_err()
        {
            info!("No previous history.");
        }
        self.editor.set_helper(Some(InputValidator));
        loop {
            let readline = self.editor.readline(LINE_PREFIX);
            match readline {
                Ok(line) => {
                    self.editor.add_history_entry(line.as_str());
                    // command_parser 是否能成功转换已经在 命令行的阶段校验了
                    let command = command_parser(line.as_str()).unwrap();

                    bincode::serialize_into::<BufWriter<&TcpStream>, Command>(
                        BufWriter::new(self.writer.get_ref()),
                        &command,
                    )?;

                    let req = bincode::deserialize_from::<BufReader<&TcpStream>, String>(
                        BufReader::new(self.reader.get_ref()),
                    )?;
                    println!("{}", &req);
                }

                Err(ReadlineError::Interrupted) => {
                    info!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    info!("CTRL-D");
                    break;
                }
                Err(err) => {
                    error!("Error: {:?}", err);
                    break;
                }
            }
        }
        self.editor
            .save_history(SERVER_CONFIG.command_history.as_str())?;
        Ok(())
    }
}

/// 客户端命令解析
///
/// insert key value
pub fn command_parser(command: &str) -> Option<Command> {
    let command_arr: Vec<String> = command
        .trim()
        .replace(";", "")
        .trim()
        .replace("\n", " ")
        .split_whitespace()
        .map(|ele| ele.to_string())
        .collect();
    return match command_arr.len() {
        // get key
        // delete key
        2 => {
            let key = command_arr.get(1).unwrap();
            match command_arr.get(0).unwrap().as_str() {
                GET => Some(Get(key.to_string())),
                DELETE => Some(Delete(key.to_string())),
                _ => None,
            }
        }
        // insert key value
        // update key value
        3 => {
            let key = command_arr.get(1).unwrap();
            let value = command_arr.get(2).unwrap();
            match command_arr.get(0).unwrap().as_str() {
                INSERT => Some(Insert(key.to_string(), value.to_string())),
                UPDATE => Some(Update(key.to_string(), value.to_string())),
                _ => None,
            }
        }
        _ => None,
    };
}

/// 客户端明命令实体
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Command {
    Get(String),
    Delete(String),
    Insert(String, String),
    Update(String, String),
}

/// 命令行附属
#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator;
impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        if !input.ends_with(';') {
            warn!("命令 [{:?}] 不完整,尝试以 ';' 结尾", input);
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
