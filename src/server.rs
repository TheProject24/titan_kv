// src/server.rs

use crate::pubsub::{ PubSub };
// use std::fmt::format;
use std::time::Duration;
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{
    // AsyncReadExt,
    AsyncWriteExt,
    // AsyncBufReadExt,
    BufReader,
};
use tokio::fs::OpenOptions;
use std::sync::Arc;
use std::time::SystemTime;

use crate::engine::{Db, Entry};
use crate::protocol::{parse_command, Command}; 
// use crate::thread_pool::ThreadPool;

async fn handle_connection(stream: TcpStream, db: Db) {
    let (read_half, mut stream) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        // NEW: Uses our dedicated RESP reader
        let parts = match crate::protocol::read_resp(&mut reader).await {
            Ok(p) if !p.is_empty() => p,
            _ => { println!("Client Disconnected."); break; }
        };

        match cage.read_line(&mut line).await {
            Ok(0) => {
                println!("Client disconnected.");
                break;
            }
            Ok(_n) => {
                if !line.ends_with('\n') {
                    eprintln!("Error: Payload too large. Discarding.");
                    break;
                }

                let command = parse_command(&line);
                println!("Parsed Command: {:?}", command);

                match command {
                    Command::SetEx(key, seconds, value) => {
                        let expiration_time  = SystemTime::now() + Duration::from_secs(seconds as u64);

                        let new_entry = Entry {
                            value: value.clone(),
                            expires_at: Some(expiration_time),
                        };

                        let mut map = db.write().await;
                        map.insert(key.clone(), new_entry);

                        

                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("database.aof")
                            .await
                            .unwrap();

                        let log = format!("SETEX {} {} {}\n", key, seconds, value);
                        file.write_all(log.as_bytes()).await.unwrap();
                        stream.write_all(b"+OK\r\n").await.unwrap();
                    }
                    Command::Ping => {
                        stream.write_all(b"+PONG\r\n").await.unwrap();
                    }
                    Command::Set(key, value) => {
                        let mut map = db.write().await;
                        let new_entry = Entry{
                            value: value.clone(),
                            expires_at: None,
                        };
                        map.insert(key.clone(), new_entry);

                        

                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("database.aof")
                            .await
                            .unwrap();

                        let log = format!("SET {} {}\n", key, value);
                        file.write_all(log.as_bytes()).await.unwrap();

                        stream.write_all(b"+OK\r\n").await.unwrap();
                    }
                    Command::Get(key) => {
                        let mut map = db.write().await;

                        match map.get(&key) {
                            Some(entry) => {
                                if let Some(expiration) = entry.expires_at {
                                    if SystemTime::now() > expiration {
                                        if map.remove(&key).is_some() {
                                            

                                            let mut file = OpenOptions::new()
                                                .create(true)
                                                .append(true)
                                                .open("database.aof")
                                                .await
                                                .unwrap();

                                                let log = format!("DEL {}\n", key);
                                                file.write_all(log.as_bytes()).await.unwrap();

                                                stream.write_all(b"$-1\r\n").await.unwrap();
                                        };


                                        continue
                                    }
                                }
                                let response = format!("+{}\r\n", entry.value);
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            None => {
                                
                                stream.write_all(b"$-1\r\n").await.unwrap();
                            }
                        }
                    }
                    Command::Del(key) => {
                        let mut map = db.write().await;
                        let not_there = map.remove(&key).is_some();

                        
                        if not_there {
                            let mut file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("database.aof")
                                .await
                                .unwrap();

                            let log = format!("DEL {}\n", key);
                            file.write_all(log.as_bytes()).await.unwrap();   
                            stream.write_all(b"+1\r\n").await.unwrap();
                        } else {
                            stream.write_all(b"+0\r\n").await.unwrap();
                        }
                    }
                    Command::Exists(key) => {
                        let map = db.read().await;

                        let key_exists = map.contains_key(&key);
                        
                        

                        if key_exists {
                            stream.write_all(b"+1\r\n").await.unwrap();
                        } else {
                            stream.write_all(b"+0\r\n").await.unwrap();
                        }
                    }
                    Command::Incr(key) => {
                        let mut map = db.write().await;
                        let current_number = match map.get(&key) {
                            Some(entry) => {
                                match entry.value.parse::<i64>() {
                                    Ok(num) => num,
                                    Err(_) => {
                                        
                                        stream.write_all(b"-Error, value is not an integer or out of range\r\n").await.unwrap();
                                        continue
                                    }
                                }
                            }
                            None => 0,
                        };
                        let new_num = current_number + 1;
                        let new_entry = Entry{
                            value: new_num.to_string(),
                            expires_at: None,
                        };
                        map.insert(key.clone(), new_entry);
                        
                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("database.aof")
                            .await
                            .unwrap();

                        let log = format!("INCR {}\n", key);
                        file.write_all(log.as_bytes()).await.unwrap();

                        let response = format!("+{}\r\n", new_num);
                        stream.write_all(response.as_bytes()).await.unwrap();
                    }
                    Command::Unknown => {
                        stream.write_all(b"-ERR unknown command\r\n").await.unwrap();
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading from stream: {}", e);
                break;
            }
            }
        }
    }

pub async fn run(address: &str, db: Db) {
    let listener = TcpListener::bind(address).await.expect("Could not bind to address");
    println!("Titan KV listening on {}", address);

    loop {
        match listener.accept().await {
            Ok((stream, _socket_addr)) => {
                let db_handle = Arc::clone(&db);

                tokio::spawn(async move {
                    handle_connection(stream, db_handle).await;   
                });
            }
            Err(e) => {
                eprintln!("Connection Failed: {}", e)
            },
        };
    }
}