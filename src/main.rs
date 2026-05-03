mod server;
mod protocol;
mod engine;
pub mod thread_pool;

use std::time::{SystemTime, Duration};
use std::fs::File;
use std::io::{BufRead, BufReader};
use crate::engine::Entry;
use crate::protocol::{parse_command, Command};

#[tokio::main]
async fn main() {
    let db = engine::new_db();

    if let Ok(file) = File::open("database.aof") {
        let reader = BufReader::new(file);
        let mut map = db.write().await;
        let mut count = 0;

        for line in reader.lines() {
            if let Ok(content) = line {
                let command = parse_command(&content);

                match command {
                    Command::Set(k, v) => {
                        let entry = Entry {
                            value: v,
                            expires_at: None
                        };
                        map.insert(k, entry);
                        count += 1;
                    },
                    Command::Del(k) => {
                        map.remove(&k);
                        count += 1;
                    }
                    Command::Incr(k) => {
                        let current = match map.get(&k) {
                            Some(entry) => entry.value.parse::<i64>().unwrap_or(0),
                            None => 0,
                        };

                        let entry = Entry {
                            value: (current + 1).to_string(),
                            expires_at: None
                        };

                        map.insert(k, entry);
                        count += 1;
                    }
                    Command::SetEx(k, s, v) => {
                        let expiration_time  = SystemTime::now() + Duration::from_secs(s as u64);

                        let new_entry = Entry {
                            value: v,
                            expires_at: Some(expiration_time),
                        };

                        map.insert(k.clone(), new_entry);
                        count += 1;
                    }
                    _ => {}
                }
            }
        }
        println!("AOF Replay Complete: Restored {} keys to memory.", count);
    }
    let address = "127.0.0.1:6379";
    server::run(address, db).await;
}
