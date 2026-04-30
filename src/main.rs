mod server;
mod protocol;
mod engine;
pub mod thread_pool;

use std::fs::File;
use std::io::{BufRead, BufReader};
use crate::protocol::{parse_command, Command};

fn main() {
    let db = engine::new_db();

    if let Ok(file) = File::open("database.aof") {
        let reader = BufReader::new(file);
        let mut map = db.write().unwrap();
        let mut count = 0;

        for line in reader.lines() {
            if let Ok(content) = line {
                let command = parse_command(&content);
                if let Command::Set(key, value) = command {
                    map.insert(key, value);
                    count += 1;
                }
            }
        }
        println!("AOF Replay Complete: Restored {} keys to memory.", count);
    }
    let address = "127.0.0.1:6379";
    server::run(address, db);
}
