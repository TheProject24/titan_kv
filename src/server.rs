// src/server.rs

use crate::pubsub::{PubSub};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{
    // AsyncReadExt, 
    AsyncWriteExt, 
    AsyncBufReadExt, 
    BufReader
};
use tokio::fs::OpenOptions;
use std::sync::Arc;
use std::time::SystemTime;


use crate::engine::{Db, Entry};
use crate::protocol::{parse_command, Command}; 
// use crate::thread_pool::ThreadPool;

async fn handle_connection(stream: TcpStream, db: Db, pubsub: PubSub) {
    let (read_half, mut stream) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(0) => {
                println!("Client Disconnected.");
                break;
            }
            Ok(_) => {
                let mut parts = Vec::new();

                if line.starts_with('*') {
                    if let Ok(num_args) = line[1..].trim().parse::<usize>() {
                        for _ in 0..num_args {
                            let mut len_line = String::new();
                            let _ = reader.read_line(&mut len_line).await;

                            let mut arg_line = String::new();
                            let _ = reader.read_line(&mut arg_line).await;

                            parts.push(arg_line.trim_end_matches("\r\n").trim_end_matches("\n").to_string());
                        }
                    }
                } else {
                    parts = line.split_whitespace().map(|s| s.to_string()).collect();
                }

                if parts.is_empty() {
                    continue;
                }

                let command = parse_command(&parts);

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
                        let _ = stream.write_all(b"+OK\r\n").await;
                    }
                    Command::Ping => {
                        let _ = stream.write_all(b"+PONG\r\n").await;
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

                        let _ = stream.write_all(b"+OK\r\n").await;
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

                                                let _ = stream.write_all(b"$-1\r\n").await;
                                        };


                                        continue
                                    }
                                }
                                let response = format!("+{}\r\n", entry.value);
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            None => {
                                
                                let _ = stream.write_all(b"$-1\r\n").await;
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
                            let _ = stream.write_all(b"$+1\r\n").await;
                        } else {
                            let _ = stream.write_all(b"$+0\r\n").await;
                        }
                    }
                    Command::Exists(key) => {
                        let map = db.read().await;

                        let key_exists = map.contains_key(&key);
                        
                        

                        if key_exists {
                            let _ = stream.write_all(b"$+1\r\n").await;
                        } else {
                            let _ = stream.write_all(b"$+0\r\n").await;
                        }
                    }
                    Command::Incr(key) => {
                        let mut map = db.write().await;
                        let current_number = match map.get(&key) {
                            Some(entry) => {
                                match entry.value.parse::<i64>() {
                                    Ok(num) => num,
                                    Err(_) => {
                                        if let Err(e) = stream.write_all(b"Erro, Value is not an integer or out of range\r\n").await {
                                            eprintln!("Client disconnected during error response: {}", e);
                                            break;
                                        }
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
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::Publish(channel, message) => {
                        crate::pubsub::handle_publish(&pubsub, &channel, &message, &mut stream).await;
                    }
                    Command::Subscribe(channel) => {
                        crate::pubsub::handle_subscribe(&pubsub, &channel, &mut stream, &mut reader).await;
                        break;
                    }
                    Command::Unsubscribe(channel) => {
                        let ack = format!(
                            "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:0\r\n",
                            channel.len(),
                            channel
                        );
                        let _ = stream.write_all(ack.as_bytes()).await;
                    }
                    Command::Unknown => {
                        if let Err(e) = stream.write_all(b"-ERR unknown command\r\n").await {
                            eprintln!("Failed to write to client: {}", e);
                            break;
                        }
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

pub async fn run(address: &str, db: Db, pubsub: PubSub) {
    let listener = TcpListener::bind(address).await.expect("Could not bind to address");
    println!("Titan KV listening on {}", address);

    loop {
        match listener.accept().await {
            Ok((stream, _socket_addr)) => {
                let db_handle = Arc::clone(&db);
                let pubsub_handle = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    handle_connection(stream, db_handle, pubsub_handle).await;   
                });
            }
            Err(e) => {
                eprintln!("Connection Failed: {}", e)
            },
        };
    }
}