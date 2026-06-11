// src/main.rs

mod server;
mod protocol;
mod engine;
pub mod logger;
pub mod thread_pool;
mod pubsub;

use std::time::{SystemTime, Duration};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use bytes::Bytes;
use crate::engine::{DataType, Entry, Db};
use crate::protocol::{parse_command, Command};

fn main() {
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    let args: Vec<String> = std::env::args().collect();
    let is_single_threaded = args.contains(&"--s-t".to_string());

    if is_single_threaded {
        crate::log_warn!("System", "Launching Titan KV in dedication SINGLE-THREADED mode.");

        if let Some(core_ids) = core_affinity::get_core_ids() {
            if let Some(first_core) = core_ids.first() {
                if core_affinity::set_for_current(*first_core) {
                    crate::log_success!("System", "Core Pinning Successful: Thread locked to CPU Core 0.");
                }
            }
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize single-threaded runtime");

        runtime.block_on(async_main());
    } else {
        crate::log_info!("System", "Launching Titan KV in standard MULTI-THREADED mode.");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize multi-threaded runtime");

        runtime.block_on(async_main());
    }
}

async fn async_main() {
    let (db, aof_rx) = engine::new_db();

    replay_aof(&db).await;

    let _aof_writer_handle = start_aof_writer(aof_rx);
    start_expiration_sweeper(db.clone());
    start_aof_compactor(db.clone());

    let address = std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let pubsub = pubsub::new_pubsub();
    server::run(&address, db, pubsub).await;
}

fn bstr(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

async fn replay_aof(db: &Db) {
    if let Ok(file) = File::open("database.aof").await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut count = 0;

        while let Ok(Some(content)) = lines.next_line().await {
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
                Command::SetEx(k, s, v) => {
                    let mut map = db.write_shard(&k).await;
                    let expiration_time = SystemTime::now() + Duration::from_secs(s as u64);

                    let new_entry = Entry {
                        value: DataType::String(v),
                        expires_at: Some(expiration_time),
                    };
                    map.insert(k, new_entry);
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
                        Some(entry) => match &entry.value {
                            DataType::String(val) => {
                                std::str::from_utf8(val).ok()
                                    .and_then(|s| s.parse::<i64>().ok()).unwrap_or(0)
                            }
                            _ => 0,
                        },
                        None => 0,
                    };

                    let entry = Entry {
                        value: DataType::String(Bytes::from((current + 1).to_string())),
                        expires_at: None,
                    };

                    map.insert(k, entry);
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
                Command::RPush(k, v) => {
                    let mut map = db.write_shard(&k).await;
                    let entry = map.entry(k).or_insert_with(|| Entry {
                        value: DataType::List(std::collections::VecDeque::new()),
                        expires_at: None,
                    });

                    if let DataType::List(list) = &mut entry.value {
                        list.push_back(v);
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
                Command::SAdd(k, member) => {
                    let mut map = db.write_shard(&k).await;
                    let entry = map.entry(k).or_insert_with(|| Entry {
                        value: DataType::Set(std::collections::HashSet::new()),
                        expires_at: None,
                    });
                    if let DataType::Set(set) = &mut entry.value {
                        set.insert(member);
                        count += 1;
                    }
                }
                _ => {}
            }
        }
        crate::log_success!("AOF", "AOF Replay Complete: Restored {} commands to memory.", count);
    }
}

fn start_expiration_sweeper(db: Db) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut all_keys_to_del = Vec::new();
            let now = SystemTime::now();

            for i in 0..db.get_shard_count() {
                let mut map = db.write_shard_by_index(i).await;
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
                    crate::log_debug!("Sweeper", "Active Expiration Swept key: {}", bstr(key));
                }
            }

            if !all_keys_to_del.is_empty() {
                if let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("database.aof").await
                {
                    for key in all_keys_to_del {
                        let log = format!("DEL {}\n", bstr(&key));
                        let _ = file.write_all(log.as_bytes()).await;
                    }
                }
            }
        }
    });
}

fn start_aof_compactor(db: Db) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            let mut new_aof_content = String::new();

            for i in 0..db.get_shard_count() {
                let map = db.read_shard_by_index(i).await;

                for (key, entry) in map.iter() {
                    let k = bstr(key);
                    match &entry.value {
                        DataType::String(val) => {
                            let v = bstr(val);
                            if let Some(expiration) = entry.expires_at {
                                if let Ok(duration) = expiration.duration_since(SystemTime::now()) {
                                    let secs = duration.as_secs();
                                    new_aof_content.push_str(&format!("SETEX {} {} \"{}\"\n", k, secs, v));
                                }
                            } else {
                                new_aof_content.push_str(&format!("SET {} \"{}\"\n", k, v));
                            }
                        }
                        DataType::List(list) => {
                            for item in list.iter().rev() {
                                new_aof_content.push_str(&format!("LPUSH {} \"{}\"\n", k, bstr(item)));
                            }
                        }
                        DataType::Hash(hmap) => {
                            for (field, value) in hmap {
                                new_aof_content.push_str(&format!("HSET {} {} \"{}\"\n",
                                    k, bstr(field), bstr(value)));
                            }
                        }
                        DataType::Set(set) => {
                            for member in set {
                                new_aof_content.push_str(&format!("SADD {} \"{}\"\n", k, bstr(member)));
                            }
                        }
                    }
                }
            }

            use tokio::fs;
            if fs::write("database.temp.aof", new_aof_content).await.is_ok() {
                if fs::rename("database.temp.aof", "database.aof").await.is_ok() {
                    crate::log_success!("AOF", "AOF Compaction complete. Log file optimized.");
                }
            }
        }
    });
}

fn start_aof_writer(mut aof_rx: tokio::sync::mpsc::Receiver<String>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("database.aof")
            .await
        {
            let mut buffer = String::with_capacity(64 * 1024);
            let mut flush_interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    Some(log) = aof_rx.recv() => {
                        buffer.push_str(&log);

                        if buffer.len() >= 64 * 1024 {
                            let _ = file.write_all(buffer.as_bytes()).await;
                            let _ = file.sync_data().await;
                            buffer.clear();
                        }
                    }

                    _ = flush_interval.tick() => {
                        if !buffer.is_empty() {
                            let _ = file.write_all(buffer.as_bytes()).await;
                            let _ = file.sync_data().await;
                            buffer.clear()
                        }
                    }
                }
            }
        } else {
            crate::log_error!("AOF Writer", "Critical: Failed to open database.aof for writing!");
        }
    })
}