// src/main.rs

mod server;
mod protocol;
mod engine;
pub mod thread_pool;
mod pubsub;

use std::time::{SystemTime, Duration};
use std::fs::File;
use std::io::{BufRead, BufReader};
use crate::engine::Entry;
use std::sync::Arc;
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
                let parts: Vec<String> = content.split_whitespace().map(|s| s.to_string()).collect();

                let command = parse_command(&parts);

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

    let db_sweeper = Arc::clone(&db);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut keys_to_del = Vec::new();
            let now = SystemTime::now();
            let mut map = db_sweeper.write().await;

            for (key, entry) in map.iter() {
                if let Some(expiration) = entry.expires_at {
                    if now > expiration {
                        keys_to_del.push(key.clone());
                    }
                }
            }

            for key in &keys_to_del {
                map.remove(key);
                println!("Active Expiration Swept key: {}", key);
            }

            drop(map);

            if !keys_to_del.is_empty() {
                use tokio::fs::OpenOptions;
                use tokio::io::AsyncWriteExt;
                
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("database.aof").await {
                    for key in keys_to_del {
                        let log = format!("DEL {}\n", key);
                        let _ = file.write_all(log.as_bytes()).await;
                    }
                }
            }
        }
    });

    let db_compact = Arc::clone(&db);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            let mut new_aof_content = String::new();

            let map = db_compact.read().await;
            for (key, entry) in map.iter() {
                if let Some(expiration) = entry.expires_at {
                    if let Ok(duration) = expiration.duration_since(SystemTime::now()) {
                        let secs = duration.as_secs();
                        new_aof_content.push_str(&format!("SETEX {} {} {}\n", key, secs, entry.value));
                    }
                } else {
                    new_aof_content.push_str(&format!("SET {} {}\n", key, entry.value));
                }
            }

            drop(map);

            use tokio::fs;
            if fs::write("database.temp.aof", new_aof_content).await.is_ok() {
                if fs::rename("database.temp.aof", "database.aof").await.is_ok() {
                    println!("AOF Compaction complete. Log file optimized.");
                }
            }
        }
    });

    let address = "127.0.0.1:6379";
    let pubsub = pubsub::new_pubsub();
    server::run(address, db, pubsub).await;
}
