mod resp;

use anyhow::{Ok, Result};

use resp::RedisValue;

use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
enum RedisCommand {
    Echo(RedisValue),
    Ping,
    Set(RedisValue, RedisValue),
    Get(RedisValue),
}

use std::collections::HashMap;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref GLOBAL_HASHMAP: Mutex<HashMap<RedisValue, RedisValue>> = Mutex::new(HashMap::new());
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:6379").await?;

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
            hashmap.insert(key.clone(), value.clone());
            eprintln!("\n\nhandle_command  {:?} -> {:?}\n", key, value);
            eprintln!("\n\nhashmap  {:?}\n", hashmap);
            None
        }
        RedisCommand::Get(key) => {
            let hashmap = GLOBAL_HASHMAP.lock().unwrap();
            if let Some(value) = hashmap.get(&key) {
                eprintln!("\n\nGot value for key {:?} -> {:?}\n", key, value);
                Some(value.clone())
            } else {
                eprintln!("\n\nNo value found for key {:?}\n", key);
                None
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
            let key = args.get(0).unwrap().clone();
            let value = args.get(1).unwrap().clone();
            Ok(RedisCommand::Set(key, value))
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
