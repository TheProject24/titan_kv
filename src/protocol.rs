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
    Unknown
}

pub fn parse_command(input: &str) -> Command {
    let clean_input = input.trim();

    let parts: Vec<&str> = clean_input.split_whitespace().collect();

    match parts[0].to_uppercase().as_str() {
        "PING" => Command::Ping,
        "GET" => {
            if parts.len() == 2 {
                Command::Get(parts[1].to_string())
            } else {
                Command::Unknown
            }
        }
        "SET" => {
            if parts.len() == 3 {
                Command::Set(parts[1].to_string(), parts[2].to_string())
            } else {
                Command::Unknown
            }
        }
        "DEL" => {
            if parts.len() == 2 {
                Command::Del(parts[1].to_string())
            } else {
                Command::Unknown
            }
        }
        "EXISTS" => {
            if parts.len() == 2 {
                Command::Exists(parts[1].to_string())
            } else {
                Command::Unknown
            }
        }
        "INCR" => {
            if parts.len() == 2 {
                Command::Incr(parts[1].to_string())
            } else {
                Command::Unknown
            }
        }
        "SETEX" => {
            if parts.len() == 4 {
                match parts[2].parse::<i64>() {
                    Ok(seconds) => Command::SetEx(parts[1].to_string(), seconds, parts[3].to_string()),
                    Err(_) => Command::Unknown,
                }
            } else {
                Command::Unknown
            }
        }
        _ => Command::Unknown
    }
}