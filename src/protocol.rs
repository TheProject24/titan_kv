// src/protocol.rs

#[derive(Debug)]
pub enum Command {
    Ping,
    Set(String, String),
    Get(String),
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
        _ => Command::Unknown
    }
}