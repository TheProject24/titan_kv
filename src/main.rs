// src/main.rs
//! Titan KV Orchestrator
//! 
//! This is the entry point for the Titan KV server. It handles:
//! 1. Runtime initialization (Single-thread core pinning vs Multi-thread work stealing).
//! 2. AOF (Append-Only-File) Replay: Restoring data from disk to the sharded HashMaps.
//! 3. Background Services: Spawning the Expiration Sweeper, AOF Writer, and Compactor.
//! 4. Signal Handling & Bootstrapping.

mod server;
mod protocol;
mod engine;
pub mod config;
pub mod logger;
pub mod thread_pool;
mod pubsub;

use std::time::{SystemTime, Duration};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use bytes::Bytes;
use crate::engine::{DataType, Entry, Db};
use crate::protocol::{parse_command, Command};
use config::Config;
use clap::Parser;

fn main() {
    // Enable ANSI colors on Windows (for the flashy terminal UI).
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    // Parse CLI arguments and environment variables.
    let config = Config::parse();

    if config.single_thread {
        // SINGLE-THREADED MODE:
        // We lock the entire process to CPU Core 0 using core_affinity.
        // This eliminates the "Context Switching" overhead and "CPU Cache Thrashes" 
        // that occur when the OS moves threads between different physically cores.
        crate::log_warn!("System", "Launching Titan KV in dedicated SINGLE-THREADED mode.");

        if let Some(core_ids) = core_affinity::get_core_ids() {
            if let Some(first_core) = core_ids.first() {
                if core_affinity::set_for_current(*first_core) {
                    crate::log_success!("System", "Core Pinning Successful: Thread locked to CPU Core 0.");
                }
            }
        }

        // Initialize a single-threaded Tokio runtime.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize single-threaded runtime");

        runtime.block_on(async_main(config));
    } else {
        // MULTI-THREADED MODE (Default):
        // Spawns N worker threads where N = number of logical CPU cores.
        // Ideal for high-throughput environments where raw CPU cycles matter more than latency variance.
        crate::log_info!("System", "Launching Titan KV in standard MULTI-THREADED mode.");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize multi-threaded runtime");

        runtime.block_on(async_main(config));
    }
}

/// The main async entry point.
async fn async_main(config: Config) {
    // Initialize the sharded database and the AOF channel.
    let (db, aof_rx) = engine::new_db();

    // 1. DATA RECOVERY: Replay the log file to bring everything back into memory.
    replay_aof(&db).await;

    // 2. DAEMON SERVICES: Spawn the background workers.
    let _aof_writer_handle = start_aof_writer(aof_rx);
    start_expiration_sweeper(db.clone());
    start_aof_compactor(db.clone());

    let address = format!("{}:{}", config.host, config.port);
    crate::log_info!("System", "Listening on {}", address);

    let pubsub = pubsub::new_pubsub();
    
    // 3. LISTEN: Block forever on the TCP accept loop.
    server::run(&address, db, pubsub, config).await;
}

/// Helper for logging bytes safely.
fn bstr(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

/// AOF Replay: The "Time Machine" of Titan KV.
/// It reads the database.aof file line-by-line and executes each write 
/// command against the in-memory shards.
async fn replay_aof(db: &Db) {
    if let Ok(file) = File::open("database.aof").await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut count = 0;

        while let Ok(Some(content)) = lines.next_line().await {
            // Treat each line in the AOF as a raw command from a client.
            let parts = crate::protocol::tokenize(&content);
            let command = parse_command(&parts);

            match command {
                Command::Set(k, v) => {
                    let mut map = db.write_shard(&k).await;
                    map.insert(k, Entry { value: DataType::String(v), expires_at: None });
                    count += 1;
                }
                Command::SetEx(k, s, v) => {
                    let mut map = db.write_shard(&k).await;
                    let expiration_time = SystemTime::now() + Duration::from_secs(s as u64);
                    map.insert(k, Entry { value: DataType::String(v), expires_at: Some(expiration_time) });
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
                        Some(e) => match &e.value {
                            DataType::String(val) => std::str::from_utf8(val).ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0),
                            _ => 0,
                        },
                        None => 0,
                    };
                    map.insert(k, Entry { value: DataType::String(Bytes::from((current + 1).to_string())), expires_at: None });
                    count += 1;
                }
                Command::LPush(k, v) => {
                    let mut map = db.write_shard(&k).await;
                    let entry = map.entry(k).or_insert_with(|| Entry { value: DataType::List(std::collections::VecDeque::new()), expires_at: None });
                    if let DataType::List(list) = &mut entry.value { list.push_front(v); count += 1; }
                }
                Command::RPush(k, v) => {
                    let mut map = db.write_shard(&k).await;
                    let entry = map.entry(k).or_insert_with(|| Entry { value: DataType::List(std::collections::VecDeque::new()), expires_at: None });
                    if let DataType::List(list) = &mut entry.value { list.push_back(v); count += 1; }
                }
                Command::LPop(k) => {
                    let mut map = db.write_shard(&k).await;
                    if let Some(entry) = map.get_mut(&k) { if let DataType::List(list) = &mut entry.value { list.pop_front(); count += 1; } }
                }
                Command::RPop(k) => {
                    let mut map = db.write_shard(&k).await;
                    if let Some(entry) = map.get_mut(&k) { if let DataType::List(list) = &mut entry.value { list.pop_back(); count += 1; } }
                }
                Command::HSet(k, f, v) => {
                    let mut map = db.write_shard(&k).await;
                    let entry = map.entry(k).or_insert_with(|| Entry { value: DataType::Hash(std::collections::HashMap::new()), expires_at: None });
                    if let DataType::Hash(hmap) = &mut entry.value { hmap.insert(f, v); count += 1; }
                }
                Command::SAdd(k, member) => {
                    let mut map = db.write_shard(&k).await;
                    let entry = map.entry(k).or_insert_with(|| Entry { value: DataType::Set(std::collections::HashSet::new()), expires_at: None });
                    if let DataType::Set(set) = &mut entry.value { set.insert(member); count += 1; }
                }
                Command::LRem(k, _c, v) => {
                    let mut map = db.write_shard(&k).await;
                    if let Some(e) = map.get_mut(&k) {
                        if let DataType::List(l) = &mut e.value {
                            if let Some(idx) = l.iter().position(|x| x == &v) { l.remove(idx); count += 1; }
                        }
                    }
                }
                Command::RPopLPush(s, d) => {
                    let mut val = None;
                    {
                        let mut map_s = db.write_shard(&s).await;
                        if let Some(e) = map_s.get_mut(&s) { if let DataType::List(l) = &mut e.value { val = l.pop_back(); } }
                    }
                    if let Some(v) = val {
                        let mut map_d = db.write_shard(&d).await;
                        let entry = map_d.entry(d).or_insert_with(|| Entry { value: DataType::List(std::collections::VecDeque::new()), expires_at: None });
                        if let DataType::List(l) = &mut entry.value { l.push_front(v); count += 1; }
                    }
                }
                _ => {}
            }
        }
        crate::log_success!("AOF", "AOF Replay Complete: Restored {} commands to memory.", count);
    }
}

/// Active Expiration Sweeper:
/// Wakes up every 10 seconds and scans every shard for expired keys. 
/// This is the "Active" part of Titan's dual-expiration strategy.
fn start_expiration_sweeper(db: Db) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut all_keys_to_del = Vec::new();
            let now = SystemTime::now();

            // We iterate through all 64 shards sequentially to avoid massive lock contention 
            // from trying to lock everything at once.
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

            // Sync the deletions to the AOF file immediately.
            if !all_keys_to_del.is_empty() {
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("database.aof").await {
                    for key in all_keys_to_del {
                        let log = format!("DEL {}\n", bstr(&key));
                        let _ = file.write_all(log.as_bytes()).await;
                    }
                }
            }
        }
    });
}

/// AOF Compactor:
/// Every 60 seconds, we create a "Snapshot" of the current memory state to prevent
/// the AOF file from growing to infinity. We write it to a temp file and then 
/// atomically swap it (rename).
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
                                    new_aof_content.push_str(&format!("SETEX {} {} \"{}\"\n", k, duration.as_secs(), v));
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
                                new_aof_content.push_str(&format!("HSET {} {} \"{}\"\n", k, bstr(field), bstr(value)));
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
            // Atomic File Replacement: Write to .temp first then rename.
            if fs::write("database.temp.aof", new_aof_content).await.is_ok() {
                if fs::rename("database.temp.aof", "database.aof").await.is_ok() {
                    crate::log_success!("AOF", "AOF Compaction complete. Log file optimized.");
                }
            }
        }
    });
}

/// AOF Background Writer:
/// Collects write logs from the MPSC channel and batches them into disk writes.
/// Every 1 second, it forces a sync (fsync) to ensure data durability.
fn start_aof_writer(mut aof_rx: tokio::sync::mpsc::Receiver<String>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("database.aof").await {
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