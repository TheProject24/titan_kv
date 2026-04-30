use std::net::{TcpListener, TcpStream};
use std::io::{BufRead, BufReader, Write, Read};
use std::sync::Arc;
use std::fs::OpenOptions;

use crate::engine::Db;
use crate::protocol::{parse_command, Command}; 
use crate::thread_pool::ThreadPool;

fn handle_connection(mut stream: TcpStream, db: Db) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();

    loop {
        line.clear();
        
        let mut cage = reader.by_ref().take(1024);

        match cage.read_line(&mut line) {
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
                    Command::Ping => {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                    Command::Set(key, value) => {
                        let mut map = db.write().unwrap();
                        map.insert(key.clone(), value.clone());

                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("database.aof")
                            .unwrap();

                        let log = format!("SET {} {}\n", key, value);
                        file.write_all(log.as_bytes()).unwrap();

                        stream.write_all(b"+OK\r\n").unwrap();
                    }
                    Command::Get(key) => {
                        let map = db.read().unwrap();
                        match map.get(&key) {
                            Some(value) => {
                                let response = format!("+{}\r\n", value);
                                stream.write_all(response.as_bytes()).unwrap();
                            }
                            None => {
                                stream.write_all(b"$-1\r\n").unwrap();
                            }
                        }
                    }
                    Command::Unknown => {
                        stream.write_all(b"-ERR unknown command\r\n").unwrap();
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

pub fn run(address: &str, db: Db) {
    let listener = TcpListener::bind(address).expect("Could not bind to address");
    println!("Titan KV listening on {}", address);

    let pool = ThreadPool::new(4);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let db_handle = Arc::clone(&db);
                
                pool.execute(move || {
                    handle_connection(s, db_handle);
                })
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}