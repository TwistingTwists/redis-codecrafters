mod resp;

use anyhow::{Error, Ok, Result};

use resp::{parse_int_with_sign, RedisValue};
use std::time::SystemTime;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone)]
enum RedisCommand {
    Echo(RedisValue),
    Ping,
    Set(RedisValue, RedisValue),
    SetTimeout(RedisValue, RedisValue, RedisValue),
    Get(RedisValue),
    Info(RedisValue),
}

use std::collections::HashMap;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref GLOBAL_HASHMAP: Mutex<HashMap<RedisValue, (RedisValue, Option<(RedisValue, SystemTime)>)>> = Mutex::new(HashMap::new());
}

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The port number to use
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    dbg!(args.port);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = handle_connection(stream).await;
        });
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
async fn handle_connection(stream: TcpStream) -> Result<()> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let value = handler.read_value().await?;
        eprintln!("Got value {:?}", value);

        let response = if let Some(v) = value {
            match to_command(extract_command(v)?) {
                Result::Ok(RedisCommand::Echo(args)) => args,
                Result::Ok(RedisCommand::Ping) => RedisValue::SimpleString("PONG".to_owned()),
                Result::Ok(RedisCommand::Set(key, value)) => {
                    let _ = handle_command(RedisCommand::Set(key, value));
                    // response to be sent to redis-client
                    RedisValue::SimpleString("OK".to_owned())
                }
                Result::Ok(RedisCommand::Get(key)) => {
                    if let Some(value) = handle_command(RedisCommand::Get(key)) {
                        value
                    } else {
                        RedisValue::SimpleString("-1".to_owned())
                    }
                }
                Result::Ok(RedisCommand::SetTimeout(key, value, timeout)) => {
                    let _ = handle_command(RedisCommand::SetTimeout(key, value, timeout));
                    RedisValue::SimpleString("OK".to_owned())
                }

                Result::Ok(info_command @ RedisCommand::Info(_)) => {
                    // Result::Ok( ref info_command @ RedisCommand::Info(ref _ic)) => {
                   handle_command(info_command.clone()).expect("BULK String expected")
                    // RedisValue::BulkString(kv_info_string.to_owned())
                }

                _c => panic!("Cannot handle command."),
            }
        } else {
            break Ok(());
        };
        eprintln!("Sending value {:?}", response);
        handler.write_value(response).await.unwrap();
    }
}

fn handle_command(command: RedisCommand) -> Option<RedisValue> {
    match command {
        RedisCommand::Set(key, value) => {
            let mut hashmap = GLOBAL_HASHMAP.lock().unwrap();
            hashmap.insert(key.clone(), (value.clone(), None));
            eprintln!("\n\nhandle_command  {:?} -> {:?}\n", key, value);
            eprintln!("\n\nhashmap  {:?}\n", hashmap);
            None
        }
        RedisCommand::SetTimeout(key, value, timeout) => {
            let mut hashmap = GLOBAL_HASHMAP.lock().unwrap();

            hashmap.insert(
                key.clone(),
                (value.clone(), Some((timeout.clone(), SystemTime::now()))),
            );
            eprintln!("\n\nhandle_command SetTimeout  {:?} -> {:?}\n", key, value);
            eprintln!("\n\nhashmap  {:?}\n", hashmap);
            None
        }
        RedisCommand::Get(key) => {
            let hashmap = GLOBAL_HASHMAP.lock().unwrap();
            if let Some(value_with_timeout) = hashmap.get(&key) {
                match value_with_timeout {
                    (value, None) => {
                        eprintln!("\n\nGot value for key {:?} -> {:?}\n", key, value);

                        Some(value.clone())
                    }
                    (value, Some((RedisValue::Integer(timeout), inserted_at))) => {
                        let elapsed = inserted_at.elapsed().expect("no time elapsed?").as_millis();
                        eprintln!("\nelapsed: {}", elapsed);
                        if elapsed > *timeout as u128 {
                            Some(RedisValue::BulkString("-1".to_owned())) // Return -1 if elapsed time is more than timeout
                        } else {
                            Some(value.clone()) // Return the original value if within timeout
                        }
                    }
                    _ => panic!("This timeout key should not be in global hashmap."),
                }
            } else {
                eprintln!("\n\nNo value found for key {:?}\n", key);
                None
            }
        }
        // RedisCommand::Info(kv_info_string) => {
        //     let mut hashmap = GLOBAL_HASHMAP.lock().unwrap();
        //     let mut kv_info = String::new();
        //     for (key, value) in hashmap.iter() {
        //         kv_info.push_str(format!("{}:{}\n", key, value).as_str());
        //     }
        //     kv_info.push_str(kv_info_string.to_owned().as_str());
        //     Some(RedisValue::BulkString(kv_info))
        // }
        RedisCommand::Info(info_command) => {
            match info_command {
                RedisValue::BulkString(s) if s.to_lowercase() == "replication" => {
                    Some(RedisValue::BulkString("role:master".to_owned()))
                    // \r\nconnected_slaves:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n".to_string())
                }
                _ => {
                    panic!(
                        "info command is not replication. it is: {:?} ",
                        info_command
                    )
                }
            }
        }
        _ => panic!("Can handle only Set command yet."),
    }
}

fn extract_command(value: RedisValue) -> Result<(String, Vec<RedisValue>)> {
    match value {
        RedisValue::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn to_command((command, args): (String, Vec<RedisValue>)) -> Result<RedisCommand> {
    match command.to_lowercase().as_str() {
        "echo" => Ok(RedisCommand::Echo(args.first().unwrap().clone())),
        "set" => {
            if args.len() < 2 {
                return Err(anyhow::anyhow!("Set command requires a key and a value"));
            }

            if args.len() == 4 {
                let key = args.get(0).unwrap().clone();
                let value = args.get(1).unwrap().clone();
                if let RedisValue::BulkString(px_command) = args.get(2).unwrap().clone() {
                    let timeout = match px_command.to_lowercase().as_str() {
                        "px" => {
                            if let RedisValue::BulkString(num_as_str) = args.get(3).unwrap().clone()
                            {
                                dbg!(&num_as_str);
                                RedisValue::Integer(
                                    parse_int_with_sign(num_as_str.as_bytes()).unwrap(),
                                )
                            } else {
                                RedisValue::Integer(1000)
                            }
                        }
                        _ => {
                            return Err(anyhow::anyhow!(
                                "cannot parse anything other than px command in set"
                            ))
                        }
                    };

                    Ok(RedisCommand::SetTimeout(key, value, timeout))
                } else {
                    panic!("px command expected but not found");
                }
            } else {
                let key = args.get(0).unwrap().clone();
                let value = args.get(1).unwrap().clone();
                Ok(RedisCommand::Set(key, value))
            }
        }
        "get" => {
            if args.len() < 1 {
                return Err(anyhow::anyhow!("get command requires a key"));
            }
            let key = args.get(0).unwrap().clone();
            Ok(RedisCommand::Get(key))
        }
        // RedisValue::SimpleString("PONG".to_string()),
        "ping" => Ok(RedisCommand::Ping),
        "info" => {
            if args.len() < 1 {
                // todo in future, return all the 'info sections'
                return Err(anyhow::anyhow!("info command assumes an argument"));
            } else {
                // return only the replication info section
                Ok(RedisCommand::Info(args.first().unwrap().clone()))
            }
        }
        // args.first().unwrap().clone(),
        c => Err(anyhow::anyhow!("Cannot parse the command given: {:?}", c)), // panic!("Cannot handle command {}", c),
    }
}

fn unpack_bulk_str(value: RedisValue) -> Result<String> {
    match value {
        RedisValue::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
