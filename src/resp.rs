use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub enum RedisValue {
    SimpleString(String),
    // Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RedisValue>),
}
pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RedisValue {
    pub fn serialize(self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", s),

            RedisValue::BulkString(s) => match s.as_str() {
                // this is null bulk string
                "-1" => format!("$-1\r\n"),
                val => format!("${}\r\n{}\r\n", s.chars().count(), val),
            },
            _ => panic!("Unsupported value for serialize"),
        }
    }
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }
    pub async fn read_value(&mut self) -> Result<Option<RedisValue>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse_message(self.buffer.split())?;
        Ok(Some(v))
    }
    pub async fn write_value(&mut self, value: RedisValue) -> Result<()> {
        self.stream.write(value.serialize().as_bytes()).await?;
        Ok(())
    }
}

fn parse_message(buffer: BytesMut) -> Result<(RedisValue, usize)> {
    // eprintln!("buffer: {:?}", buffer);
    match buffer[0] as char {
        // ':' => parse_integer(&buffer),
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(RedisValue, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();
        return Ok((RedisValue::SimpleString(string), len + 1));
    }
    return Err(anyhow::anyhow!("Invalid string {:?}", buffer));
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(RedisValue, usize)> {
    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let bulk_str_len = parse_int(line)?;
        (bulk_str_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };
    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;
    Ok((
        RedisValue::BulkString(String::from_utf8(
            buffer[bytes_consumed..end_of_bulk_str].to_vec(),
        )?),
        total_parsed,
    ))
}

fn parse_array(buffer: BytesMut) -> Result<(RedisValue, usize)> {
    let (array_length, mut bytes_consumed) =
        if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
            let array_length = parse_int(line)?;
            (array_length, len + 1)
        } else {
            return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
        };
    let mut items = vec![];
    for _ in 0..array_length {
        let (array_item, len) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        items.push(array_item);
        bytes_consumed += len;
    }
    return Ok((RedisValue::Array(items), bytes_consumed));
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}

pub fn parse_integer(buffer: &[u8]) -> Result<(RedisValue, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        if let Ok(int_val) = parse_int_with_sign(line) {
            return Ok((RedisValue::Integer(int_val), len + 1));
        }
    }
    return Err(anyhow::anyhow!("Invalid integer {:?}", buffer));
}

pub fn parse_int_with_sign(line: &[u8]) -> Result<i64> {
    if line.is_empty() {
        return Err(anyhow::anyhow!("Empty integer value"));
    }

    let (sign, start_index) = match line[0] {
        b'+' => (1, 1),
        b'-' => (-1, 1),
        _ => (1, 0),
    };

    let unsigned_val = std::str::from_utf8(&line[start_index..])?
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Invalid integer: {}", e))?;

    Ok(sign * (unsigned_val as i64))
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(std::str::from_utf8(buffer)?.parse::<i64>()?)
}
