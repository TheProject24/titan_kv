use std::net::{TcpListener, TcpStream};
use std::io::{BufRead, BufReader, Write}; // Removed 'Read', we only need BufRead
use std::sync::Arc;
use std::thread;

use crate::engine::Db;
use crate::protocol::parse_command; 

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

                stream.write_all(b"+OK\r\n").unwrap();
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

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let db_handle = Arc::clone(&db);
                
                thread::spawn(move || {
                    handle_connection(s, db_handle);    
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}