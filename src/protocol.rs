// src/protocol.rs

use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub enum Command {
    Ping,
    Set(String, String),
    Get(String),
    Del(String),
    Exists(String),
    Incr(String),
    SetEx(String, i64, String),
    Publish(String, String),
    Subscribe(String),
    Unsubscribe(String),
    LPush(String, String),
    LPop(String),
    RPop(String),
    RPush(String, String),
    RPopLPush(String, String),
    LRem(String, i64, String),
    LTrim(String, i32, i32),
    LRange(String, i32, i32),
    MGet(String, String),
    SAdd(String, String),
    SInter(String, String),
    HSet(String, String, String),
    HGetAll(String),
    HGet(String, String),
    Keys(String),
    Scan(usize, Option<String>),
    Unknown,
}

pub fn parse_command(parts: &[String]) -> Command {
    if parts.is_empty() {
        return Command::Unknown;
    }

    match parts[0].to_uppercase().as_str() {
        "PING" => Command::Ping,
        "GET" if parts.len() == 2 => Command::Get(parts[1].clone()),
        "SET" if parts.len() == 3 => Command::Set(parts[1].clone(), parts[2].clone()),
        "DEL" if parts.len() == 2 => Command::Del(parts[1].clone()),
        "EXISTS" if parts.len() == 2 => Command::Exists(parts[1].clone()),
        "INCR" if parts.len() == 2 => Command::Incr(parts[1].clone()),
        "SETEX" if parts.len() == 4 => {
            match parts[2].parse::<i64>() {
                Ok(seconds) => Command::SetEx(parts[1].clone(), seconds, parts[3].clone()),
                Err(_) => Command::Unknown,
            }
        }
        "PUBLISH" if parts.len() == 3 => Command::Publish(parts[1].clone(), parts[2].clone()),
        "SUBSCRIBE" if parts.len() == 2 => Command::Subscribe(parts[1].clone()),
        "UNSUBSCRIBE" if parts.len() == 2 => Command::Unsubscribe(parts[1].clone()),
        "LPUSH" if parts.len() == 3 => Command::LPush(parts[1].clone(), parts[2].clone()),
        "LPOP" if parts.len() == 2 => Command::LPop(parts[1].clone()),
        "RPOP" if parts.len() == 2 => Command::RPop(parts[1].clone()),
        "RPUSH" if parts.len() == 3 => Command::RPush(parts[1].clone(), parts[2].clone()),
        "RPOPLPUSH" if parts.len() == 3 => Command::RPopLPush(parts[1].clone(), parts[2].clone()),
        "LREM" if parts.len() == 4 => {
            let count = parts[2].parse::<i64>().unwrap_or(1);
            Command::LRem(parts[1].clone(), count, parts[2].clone())
        }
        "LTRIM" if parts.len() == 4 => {
            match (parts[2].parse::<i32>(), parts[3].parse::<i32>()) {
                (Ok(start), Ok(stop)) => Command::LTrim(parts[1].clone(), start, stop),
                _ => Command::Unknown,
            }
        }
        "LRANGE" if parts.len() == 4 => {
            match (parts[2].parse::<i32>(), parts[3].parse::<i32>()) {
                (Ok(start), Ok(stop)) => Command::LRange(parts[1].clone(), start, stop),
                _ => Command::Unknown,
            }
        }
        "HSET" if parts.len() == 4 =>
            Command::HSet(parts[1].clone(), parts[2].clone(), parts[3].clone()),
        "HGETALL" if parts.len() == 2 => Command::HGetAll(parts[1].clone()),
        "HGET" if parts.len() == 3 => Command::HGet(parts[1].clone(), parts[2].clone()),
        "MGET" if parts.len() == 3 => Command::MGet(parts[1].clone(), parts[2].clone()),
        "SADD" if parts.len() == 3 => Command::SAdd(parts[1].clone(), parts[2].clone()),
        "SINTER" if parts.len() == 3 => Command::SInter(parts[1].clone(), parts[2].clone()),
        "KEYS" if parts.len() == 2 => Command::Keys(parts[1].clone()),
        "SCAN" if parts.len() >= 2 => {
            let cursor = parts[1].parse::<usize>().unwrap_or(0);

            let mut match_pattern = None;

            if parts.len() == 4 && parts[2].to_uppercase() == "MATCH" {
                match_pattern = Some(parts[3].clone());
            }

            Command::Scan(cursor, match_pattern)
         }
        _ => Command::Unknown,
    }
}

pub fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
            }
            ' ' | '\t' | '\n' if !in_quotes => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

pub async fn read_resp<R: tokio::io::AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if !line.starts_with('*'){
        return Ok(crate::protocol::tokenize(&line));
    }

    let count = line.trim_start_matches('*').trim().parse::<usize>()?;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        let mut len_line = String::new();
        reader.read_line(&mut len_line).await?;

        if !len_line.starts_with('$') { continue; }
        let len = len_line.trim_start_matches('$').trim().parse::<usize>()?;

        let mut buf = vec![0u8; len + 2];
        reader.read_exact(&mut buf).await?;

        let arg = String::from_utf8_lossy(&buf[..len]).to_string();
        args.push(arg);
    }

    Ok(args)
}