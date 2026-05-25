// src/main.rs

mod server;
mod protocol;
mod engine;
pub mod logger;
pub mod thread_pool;
mod pubsub;

use std::time::{ SystemTime, Duration };
use std::fs::File;
use std::io::{ BufRead, BufReader };
use crate::engine::{ DataType, Entry };
use crate::protocol::{ parse_command, Command };

#[tokio::main]
async fn main() {
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    let db = engine::new_db();

    if let Ok(file) = File::open("database.aof") {
        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines() {
            if let Ok(content) = line {
                let parts = crate::protocol::tokenize(&content);

                let command = parse_command(&parts);

                match command {
                    Command::Set(k, v) => {
                        let mut map = db.write_shard(&k).await;
                        let entry = Entry {
                            value: DataType::String(v),
                            expires_at: None,
                        };
                        map.insert(k, entry);
                        count += 1;
                    }
                    Command::Del(k) => {
                        let mut map = db.write_shard(&k).await;
                        map.remove(&k);
                        count += 1;
                    }
                    Command::Incr(k) => {
                        let mut map = db.write_shard(&k).await;
                        let current = match map.get(&k) {
                            Some(entry) => {
                                match &entry.value {
                                    DataType::String(val) => val.parse::<i64>().unwrap_or(0),
                                    _ => 0,
                                }
                            }
                            None => 0,
                        };

                        let entry = Entry {
                            value: DataType::String((current + 1).to_string()),
                            expires_at: None,
                        };

                        map.insert(k, entry);
                        count += 1;
                    }
                    Command::SetEx(k, s, v) => {
                        let mut map = db.write_shard(&k).await;
                        let expiration_time = SystemTime::now() + Duration::from_secs(s as u64);

                        let new_entry = Entry {
                            value: DataType::String(v),
                            expires_at: Some(expiration_time),
                        };

                        map.insert(k.clone(), new_entry);
                        count += 1;
                    }
                    Command::LPush(k, v) => {
                        let mut map = db.write_shard(&k).await;
                        let entry = map.entry(k).or_insert_with(|| Entry {
                            value: DataType::List(std::collections::VecDeque::new()),
                            expires_at: None,
                        });

                        if let DataType::List(list) = &mut entry.value {
                            list.push_front(v);
                            count += 1;
                        }
                    }
                    Command::LPop(k) => {
                        let mut map = db.write_shard(&k).await;
                        if let Some(entry) = map.get_mut(&k) {
                            if let DataType::List(list) = &mut entry.value {
                                list.pop_front();
                                count += 1;
                            }
                        }
                    }
                    Command::HSet(k, f, v) => {
                        let mut map = db.write_shard(&k).await;
                        let entry = map.entry(k).or_insert_with(|| Entry {
                            value: DataType::Hash(std::collections::HashMap::new()),
                            expires_at: None,
                        });
                        if let DataType::Hash(hmap) = &mut entry.value {
                            hmap.insert(f, v);
                            count += 1;
                        }
                    }
                    _ => {}
                }
            }
        }
        log_success!("AOF", "AOF Replay Complete: Restored {} keys to memory.", count);
    }

    let db_sweeper = db.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut all_keys_to_del = Vec::new();
            let now = SystemTime::now();

            for i in 0..db_sweeper.get_shard_count() {
                let mut map = db_sweeper.write_shard_by_index(i).await;
                let mut shard_keys_to_del = Vec::new();

                for (key, entry) in map.iter() {
                    if let Some(expiration) = entry.expires_at {
                        if now > expiration {
                            shard_keys_to_del.push(key.clone());
                            all_keys_to_del.push(key.clone());
                        }
                    }
                }

                for key in &shard_keys_to_del {
                    map.remove(key);
                    crate::log_debug!("Sweeper", "Active Expiration Swept key: {}", key);
                }
            }

            if !all_keys_to_del.is_empty() {
                use tokio::fs::OpenOptions;
                use tokio::io::AsyncWriteExt;

                if
                    let Ok(mut file) = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("database.aof").await
                {
                    for key in all_keys_to_del {
                        let log = format!("DEL {}\n", key);
                        let _ = file.write_all(log.as_bytes()).await;
                    }
                }
            }
        }
    });

    let db_compact = db.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            let mut new_aof_content = String::new();

            for i in 0..db_compact.get_shard_count() {
                let map = db_compact.read_shard_by_index(i).await;

                for (key, entry) in map.iter() {
                    match &entry.value {
                        DataType::String(val) => {
                            if let Some(expiration) = entry.expires_at {
                                if let Ok(duration) = expiration.duration_since(SystemTime::now()) {
                                    let secs = duration.as_secs();
                                    new_aof_content.push_str(
                                        &format!("SETEX {} {} \"{}\"\n", key, secs, val)
                                    );
                                }
                            } else {
                                new_aof_content.push_str(&format!("SET {} \"{}\"\n", key, val));
                            }
                        }

                        DataType::List(list) => {
                            for item in list.iter().rev() {
                                new_aof_content.push_str(&format!("LPUSH {} \"{}\"\n", key, item));
                            }
                        }

                        DataType::Hash(hmap) => {
                            for (field, value) in hmap {
                                new_aof_content.push_str(&format!("HSET {} {} \"{}\"\n", key, field, value));
                            }
                        }
                    }
                }
            }

            use tokio::fs;
            if fs::write("database.temp.aof", new_aof_content).await.is_ok() {
                if fs::rename("database.temp.aof", "database.aof").await.is_ok() {
                    log_success!("AOF", "AOF Compaction complete. Log file optimized.");
                }
            }
        }
    });

    // let db_compact = Arc::clone(&db);
    // tokio::spawn(async move {
    //     loop {
    //         tokio::time::sleep(Duration::from_secs(60)).await;

    //         let mut new_aof_content = String::new();

    //         let map = db_compact.read().await;
    //         for (key, entry) in map.iter() {
    //             if let DataType::String(val) = &entry.value {
    //                 if let Some(expiration) = entry.expires_at {
    //                     if let Ok(duration) = expiration.duration_since(SystemTime::now()) {
    //                         let secs = duration.as_secs();
    //                         new_aof_content.push_str(&format!("SETEX {} {} {}\n", key, secs, val));
    //                     }
    //                 } else {
    //                     new_aof_content.push_str(&format!("SET {} {}\n", key, val));
    //                 }
    //             }
    //         }

    //         drop(map);

    //         use tokio::fs;
    //         if fs::write("database.temp.aof", new_aof_content).await.is_ok() {
    //             if fs::rename("database.temp.aof", "database.aof").await.is_ok() {
    //                 println!("AOF Compaction complete. Log file optimized.");
    //             }
    //         }
    //     }
    // });

    let address = "127.0.0.1:6379";
    let pubsub = pubsub::new_pubsub();
    server::run(address, db, pubsub).await;
}
