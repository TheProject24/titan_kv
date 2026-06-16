// src/protocol.rs

use bytes::Bytes;
use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub enum Command {
    Auth(String),
    Ping,
    Set(Bytes, Bytes),
    Get(Bytes),
    Del(Bytes),
    Exists(Bytes),
    Incr(Bytes),
    SetEx(Bytes, i64, Bytes),
    Publish(Bytes, Bytes),
    Subscribe(Bytes),
    Unsubscribe(Bytes),
    LPush(Bytes, Bytes),
    LPop(Bytes),
    RPop(Bytes),
    RPush(Bytes, Bytes),
    RPopLPush(Bytes, Bytes),
    LRem(Bytes, i64, Bytes),
    LTrim(Bytes, i32, i32),
    LRange(Bytes, i32, i32),
    MGet(Bytes, Bytes),
    SAdd(Bytes, Bytes),
    SInter(Bytes, Bytes),
    HSet(Bytes, Bytes, Bytes),
    HGetAll(Bytes),
    HGet(Bytes, Bytes),
    Keys(Bytes),
    Scan(usize, Option<Bytes>),
    Info,
    Type(Bytes),
    Ttl(Bytes),
    ClientList,
    Client,
    Monitor,
    Pttl(Bytes),
    Memory,
    Llen(Bytes),
    Hlen(Bytes),
    Scard(Bytes),
    SMembers(Bytes),
    Strlen(Bytes),
    DbSize,
    Unknown,
}

pub fn parse_command(parts: &[Bytes]) -> Command {
    if parts.is_empty() {
        return Command::Unknown;
    }

    // Uppercase only the command verb — stack-allocated for short names
    let cmd: Vec<u8> = parts[0].iter().map(|b| b.to_ascii_uppercase()).collect();

    match cmd.as_slice() {
        b"AUTH" if parts.len() == 2 => Command::Auth(parts[1].string()),
        b"PING" => Command::Ping,
        b"GET" if parts.len() == 2 => Command::Get(parts[1].clone()),
        b"SET" if parts.len() == 3 => Command::Set(parts[1].clone(), parts[2].clone()),
        b"DEL" if parts.len() == 2 => Command::Del(parts[1].clone()),
        b"EXISTS" if parts.len() == 2 => Command::Exists(parts[1].clone()),
        b"INCR" if parts.len() == 2 => Command::Incr(parts[1].clone()),
        b"SETEX" if parts.len() == 4 => {
            match std::str::from_utf8(&parts[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(seconds) => Command::SetEx(parts[1].clone(), seconds, parts[3].clone()),
                None => Command::Unknown,
            }
        }
        b"PUBLISH" if parts.len() == 3 => Command::Publish(parts[1].clone(), parts[2].clone()),
        b"SUBSCRIBE" if parts.len() == 2 => Command::Subscribe(parts[1].clone()),
        b"UNSUBSCRIBE" if parts.len() == 2 => Command::Unsubscribe(parts[1].clone()),
        b"LPUSH" if parts.len() == 3 => Command::LPush(parts[1].clone(), parts[2].clone()),
        b"LPOP" if parts.len() == 2 => Command::LPop(parts[1].clone()),
        b"RPOP" if parts.len() == 2 => Command::RPop(parts[1].clone()),
        b"RPUSH" if parts.len() == 3 => Command::RPush(parts[1].clone(), parts[2].clone()),
        b"RPOPLPUSH" if parts.len() == 3 => Command::RPopLPush(parts[1].clone(), parts[2].clone()),
        b"LREM" if parts.len() == 4 => {
            let count = std::str::from_utf8(&parts[2]).ok()
                .and_then(|s| s.parse::<i64>().ok()).unwrap_or(1);
            Command::LRem(parts[1].clone(), count, parts[3].clone())
        }
        b"LTRIM" if parts.len() == 4 => {
            match (
                std::str::from_utf8(&parts[2]).ok().and_then(|s| s.parse::<i32>().ok()),
                std::str::from_utf8(&parts[3]).ok().and_then(|s| s.parse::<i32>().ok()),
            ) {
                (Some(start), Some(stop)) => Command::LTrim(parts[1].clone(), start, stop),
                _ => Command::Unknown,
            }
        }
        b"LRANGE" if parts.len() == 4 => {
            match (
                std::str::from_utf8(&parts[2]).ok().and_then(|s| s.parse::<i32>().ok()),
                std::str::from_utf8(&parts[3]).ok().and_then(|s| s.parse::<i32>().ok()),
            ) {
                (Some(start), Some(stop)) => Command::LRange(parts[1].clone(), start, stop),
                _ => Command::Unknown,
            }
        }
        b"HSET" if parts.len() == 4 =>
            Command::HSet(parts[1].clone(), parts[2].clone(), parts[3].clone()),
        b"HGETALL" if parts.len() == 2 => Command::HGetAll(parts[1].clone()),
        b"HGET" if parts.len() == 3 => Command::HGet(parts[1].clone(), parts[2].clone()),
        b"MGET" if parts.len() == 3 => Command::MGet(parts[1].clone(), parts[2].clone()),
        b"SADD" if parts.len() == 3 => Command::SAdd(parts[1].clone(), parts[2].clone()),
        b"SINTER" if parts.len() == 3 => Command::SInter(parts[1].clone(), parts[2].clone()),
        b"KEYS" if parts.len() == 2 => Command::Keys(parts[1].clone()),
        b"SCAN" if parts.len() >= 2 => {
            let cursor = std::str::from_utf8(&parts[1]).ok()
                .and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);

            let mut match_pattern = None;
            if parts.len() == 4 {
                let kw: Vec<u8> = parts[2].iter().map(|b| b.to_ascii_uppercase()).collect();
                if kw.as_slice() == b"MATCH" {
                    match_pattern = Some(parts[3].clone());
                }
            }

            Command::Scan(cursor, match_pattern)
        }
        b"MONITOR" if parts.len() == 1 => Command::Monitor,
        b"INFO" => Command::Info,
        b"TYPE" if parts.len() == 2 => Command::Type(parts[1].clone()),
        b"TTL" if parts.len() == 2 => Command::Ttl(parts[1].clone()),
        b"PTTL" if parts.len() == 2 => Command::Pttl(parts[1].clone()),
        b"MEMORY" if parts.len() >= 2 => {
            let sub: Vec<u8> = parts[1].iter().map(|b| b.to_ascii_uppercase()).collect();
            if sub.as_slice() == b"USAGE" { Command::Memory } else { Command::Unknown }
        }
        b"LLEN" if parts.len() == 2 => Command::Llen(parts[1].clone()),
        b"HLEN" if parts.len() == 2 => Command::Hlen(parts[1].clone()),
        b"SCARD" if parts.len() == 2 => Command::Scard(parts[1].clone()),
        b"SMEMBERS" if parts.len() == 2 => Command::SMembers(parts[1].clone()),
        b"CLIENT" if parts.len() >= 2 => {
            let sub: Vec<u8> = parts[1].iter().map(|b| b.to_ascii_uppercase()).collect();
            if sub.as_slice() == b"LIST" { Command::ClientList } else { Command::Client }
        }
        b"STRLEN" if parts.len() == 2 => Command::Strlen(parts[1].clone()),
        b"DBSIZE" => Command::DbSize,
        b"CLIENT" => Command::Client,
        _ => Command::Unknown,
    }
}

// Bytes::from(String) is zero-copy — it takes ownership of the string's heap buffer.
pub fn tokenize(input: &str) -> Vec<Bytes> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in input.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
            }
            ' ' | '\t' | '\n' if !in_quotes => {
                if !current.is_empty() {
                    tokens.push(Bytes::from(std::mem::take(&mut current)));
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        tokens.push(Bytes::from(current));
    }
    tokens
}

pub async fn read_resp<R: tokio::io::AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> Result<Vec<Bytes>, Box<dyn std::error::Error>> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if !line.starts_with('*') {
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

        // One allocation here; every subsequent clone throughout the pipeline is O(1).
        let arg = Bytes::copy_from_slice(&buf[..len]);
        args.push(arg);
    }

    Ok(args)
}
