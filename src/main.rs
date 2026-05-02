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

                match command {
                    Command::Set(k, v) => {
                        map.insert(k, v);
                        count += 1;
                    },
                    Command::Del(k) => {
                        map.remove(&k);
                        count += 1;
                    }
                    Command::Incr(k) => {
                        let current = match map.get(&k) {
                            Some(value_string) => value_string.parse::<i64>().unwrap_or(0),
                            None => 0,
                        };

                        map.insert(k, (current + 1).to_string());
                        count += 1;
                    }
                    _ => {}
                }
                // if let Command::Set(key, value) = command {
                //     map.insert(key, value);
                //     count += 1;
                // }
            }
        }
        println!("AOF Replay Complete: Restored {} keys to memory.", count);
    }
    let address = "127.0.0.1:6379";
    server::run(address, db);
}
