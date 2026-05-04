// src/protocol.rs

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
    Unknown
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
        _ => Command::Unknown
    }
}