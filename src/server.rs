// src/server.rs

use crate::pubsub::{ PubSub };
// use std::fmt::format;
use std::time::Duration;
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{
    // AsyncReadExt,
    AsyncWriteExt,
    // AsyncBufReadExt,
    BufReader,
};
use tokio::fs::OpenOptions;
use std::sync::Arc;
use std::time::SystemTime;

async fn append_aof(log: String) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("database.aof").await
    {
        let _ = file.write_all(log.as_bytes()).await;
    } else {
        crate::log_error!("AOF", "Failed to append to AOF file (Access Denied/Locked)");
    }
}

use crate::engine::{ self, Db, Entry, MultiWriteGuard, MultiReadGuard };
use crate::protocol::{ parse_command, Command, read_resp };
// use crate::thread_pool::ThreadPool;

async fn handle_connection(stream: TcpStream, db: Db, pubsub: PubSub) {
    let (read_half, mut stream) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        // NEW: Uses our dedicated RESP reader
        let parts: Vec<String> = match read_resp(&mut reader).await {
            Ok(p) if !p.is_empty() => p,
            _ => { crate::log_info!("Client", "Client Disconnected."); break; }
        };

        let command = parse_command(&parts);
        let summary_parts: Vec<String> = parts.iter().map(|p| {
            if p.len() > 30 {
                format!("{}...({}b)", &p[..15], p.len())
            } else {
                p.clone()
            }
        }).collect();
        crate::log_info!("Command", "{}", summary_parts.join(" "));

        match command {
            Command::SetEx(key, seconds, value) => {
                        let expiration_time =
                            SystemTime::now() + Duration::from_secs(seconds as u64);

                        let new_entry = Entry {
                            value: crate::engine::DataType::String(value.clone()),
                            expires_at: Some(expiration_time),
                        };

                        let mut map = db.write_shard(&key).await;
                        map.insert(key.clone(), new_entry);

                        let log = format!("SETEX {} {} {}\n", key, seconds, value);
                        append_aof(log).await;
                        let _ = stream.write_all(b"+OK\r\n").await;
                    }
            Command::Ping => {
                        let _ = stream.write_all(b"+PONG\r\n").await;
                    }
            Command::Set(key, value) => {
                        let mut shard = db.write_shard(&key).await;

                        shard.insert(key, Entry {
                            value: engine::DataType::String(value),
                            expires_at: None,
                        });

                        let _ = stream.write_all(b"+OK\r\n").await;
                        // let new_entry = Entry {
                        //     value: crate::engine::DataType::String(value.clone()),
                        //     expires_at: None,
                        // };
                        // map.insert(key.clone(), new_entry);

                        // let log = format!("SET {} {}\n", key, value);
                        // append_aof(log).await;

                        // let _ = stream.write_all(b"+OK\r\n").await;
                    }
            Command::Get(key) => {
                        let mut map = db.write_shard(&key).await;

                        match map.get(&key) {
                            Some(entry) => {
                                if let Some(expiration) = entry.expires_at {
                                    if SystemTime::now() > expiration {
                                        if map.remove(&key).is_some() {
                                            let log = format!("DEL {}\n", key);
                                            append_aof(log).await;

                                            let _ = stream.write_all(b"$-1\r\n").await;
                                        }

                                        continue;
                                    }
                                }
                                match &entry.value {
                                    crate::engine::DataType::String(val) => {
                                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    }
                                    _ => {
                                        let _ = stream.write_all(
                                            b"-WRONGTYPE Operation against a key holding the wrong type of value\r\n"
                                        ).await;
                                    }
                                }
                            }
                            None => {
                                let _ = stream.write_all(b"$-1\r\n").await;
                            }
                        }
                    }
            Command::Del(key) => {
                        let mut map = db.write_shard(&key).await;
                        let not_there = map.remove(&key).is_some();

                        if not_there {
                            let log = format!("DEL {}\n", key);
                            append_aof(log).await;
                            let _ = stream.write_all(b":1\r\n").await;
                        } else {
                            let _ = stream.write_all(b":0\r\n").await;
                        }
                    }
            Command::Exists(key) => {
                        let map = db.read_shard(&key).await;

                        let key_exists = map.contains_key(&key);

                        if key_exists {
                            let _ = stream.write_all(b":1\r\n").await;
                        } else {
                            let _ = stream.write_all(b":0\r\n").await;
                        }
                    }
            Command::Incr(key) => {
                        let mut map = db.write_shard(&key).await;
                        let current_number = match map.get(&key) {
                            Some(entry) => {
                                match &entry.value {
                                    crate::engine::DataType::String(val) => {
                                        match val.parse::<i64>() {
                                            Ok(num) => num,
                                            Err(_) => {
                                                if
                                                    let Err(e) = stream.write_all(
                                                        b"-ERR Value is not an integer or out of range\r\n"
                                                    ).await
                                                {
                                                    crate::log_error!("Client", "Client disconnected during error response: {}", e);
                                                    break;
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                    _ => {
                                        let _ = stream.write_all(
                                            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                                        ).await;
                                        continue;
                                    }
                                }
                            }
                            None => 0,
                        };
                        let new_num = current_number + 1;
                        let new_entry = Entry {
                            value: crate::engine::DataType::String(new_num.to_string()),
                            expires_at: None,
                        };
                        map.insert(key.clone(), new_entry);

                        let log = format!("INCR {}\n", key);
                        append_aof(log).await;

                        let response = format!(":{}\r\n", new_num);
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
            Command::Publish(channel, message) => {
                        crate::pubsub::handle_publish(
                            &pubsub,
                            &channel,
                            &message,
                            &mut stream
                        ).await;
                    }
            Command::Subscribe(channel) => {
                        crate::pubsub::handle_subscribe(
                            &pubsub,
                            &channel,
                            &mut stream,
                            &mut reader
                        ).await;
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
            Command::LPush(key, value) => {
                        let mut map = db.write_shard(&key).await;

                        let entry = map.entry(key.clone()).or_insert_with(|| Entry {
                            value: crate::engine::DataType::List(std::collections::VecDeque::new()),
                            expires_at: None,
                        });

                        match &mut entry.value {
                            crate::engine::DataType::List(list) => {
                                list.push_front(value.clone());
                                let len = list.len();

                                let log = format!("LPUSH {} \"{}\"\n", key, value);
                                append_aof(log).await;

                                let response = format!(":{}\r\n", len);
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            _ => {
                                let _ = stream.write_all(
                                    b"-WRONGTYPE Operation against a key holding the wrong kind of value \r\n"
                                ).await;
                            }
                        }
                    }
            Command::LPop(key) => {
                        let mut map = db.write_shard(&key).await;

                        if let Some(entry) = map.get_mut(&key) {
                            match &mut entry.value {
                                crate::engine::DataType::List(list) => {
                                    if let Some(val) = list.pop_front() {
                                        let log = format!("LPOP {}\n", key);
                                        append_aof(log).await;

                                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    } else {
                                        let _ = stream.write_all(b"$-1\r\n").await;
                                    }
                                }
                                _ => {
                                    let _ = stream.write_all(
                                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                                    ).await;
                                }
                            }
                        } else {
                            let _ = stream.write_all(b"$-1\r\n").await;
                        }
                    }
            Command::RPop(key) => {
                        let mut map = db.write_shard(&key).await;

                        if let Some(entry) = map.get_mut(&key) {
                            match &mut entry.value {
                                crate::engine::DataType::List(list) => {
                                    if let Some(val) = list.pop_back() {
                                        let log = format!("RPOP {}\n", key);
                                        append_aof(log).await;

                                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    } else {
                                        let _ = stream.write_all(b"$-1\r\n").await;
                                    }
                                }
                                _ => {
                                    let _ = stream.write_all(
                                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                                    ).await;
                                }
                            }
                        } else {
                            let _ = stream.write_all(b"$-1\r\n").await;
                        }
                    }
            Command::LTrim(key, start, stop) => {
                        let mut map = db.write_shard(&key).await;
                    
                        if let Some(entry) = map.get_mut(&key) {
                            match &mut entry.value {
                                crate::engine::DataType::List(list) => {
                                    let len = list.len() as i32;
                                    
                                    let mut s = if start < 0 { len + start } else { start };
                                    let mut e = if stop < 0 { len + stop } else { stop };
                    
                                    if s > e || s >= len {
                                        map.remove(&key);
                                    } else {
                                        s = std::cmp::max(0, s);
                                        e = std::cmp::min(e, len - 1);
                    
                                        while list.len() as i32 > e + 1 {
                                            list.pop_back();
                                        }
                                        for _ in 0..s {
                                            list.pop_front();
                                        }
                    
                                        if list.is_empty() {
                                            map.remove(&key);
                                        }
                                    }
                    
                                    let log = format!("LTRIM {} {} {}\n", key, start, stop);
                                    append_aof(log).await;
                    
                                    let _ = stream.write_all(b"+OK\r\n").await;
                                }
                                _ => {
                                    let _ = stream.write_all(
                                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                                    ).await;
                                }
                            }
                        } else {
                            let _ = stream.write_all(b"+OK\r\n").await;
                        }
                    }
            Command::LRange(key, start, stop) => {
                        let map = db.read_shard(&key).await;

                        if let Some(entry) = map.get(&key) {
                            match &entry.value {
                                crate::engine::DataType::List(list) => {
                                    let len = list.len() as i32;
                                    
                                    let s = if start < 0 { len + start } else { start };
                                    let e = if stop < 0 { len + stop } else { stop };
                    
                                    if s > e || s >= len || e < 0 {
                                        let _ = stream.write_all(b"*0\r\n").await;
                                    } else {
                                        let s = std::cmp::max(0, s) as usize;
                                        let e = std::cmp::min(e, len - 1) as usize;
                    
                                        let mut response = format!("*{}\r\n", e - s + 1);
                                        for i in s..=e {
                                            if let Some(val) = list.get(i) {
                                                response.push_str(&format!("${}\r\n{}\r\n", val.len(), val));
                                            }
                                        }
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    }
                                }
                                _ => {
                                    let _ = stream.write_all(
                                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                                    ).await;
                                }
                            }
                        } else {
                            let _ = stream.write_all(b"*0\r\n").await;
                        }
                    }
            Command::RPush(key, value) => {
                        let mut map = db.write_shard(&key).await;
                        let entry = map.entry(key.clone()).or_insert_with(|| Entry {
                            value: crate::engine::DataType::List(std::collections::VecDeque::new()),
                            expires_at: None,
                        });

                        if let crate::engine::DataType::List(list) = &mut entry.value {
                            list.push_back(value.clone());
                            let len = list.len();

                            let log = format!("RPUSH {} \"{}\"\n", key, value);
                            append_aof(log).await;

                            let response = format!(":{}\r\n", len);
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                    }
            Command::HSet(key, field, value) => {
                        let mut map = db.write_shard(&key).await;
                        let entry = map.entry(key.clone()).or_insert_with(|| Entry {
                            value: crate::engine::DataType::Hash(std::collections::HashMap::new()),
                            expires_at: None,
                        });
                        if let crate::engine::DataType::Hash(hmap) = &mut entry.value {
                            hmap.insert(field.clone(), value.clone());
                            let log = format!("HSET {} {} \"{}\"\n", key, field, value);
                            append_aof(log).await;

                            let response = format!("+OK\r\n");
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else {
                            let _ = stream.write_all(b"-WRONGTYPE\r\n").await;
                        }
                    }
            Command::HGetAll(key) => {
                        let map = db.read_shard(&key).await;
                        if let Some(entry) = map.get(&key) {
                            if let crate::engine::DataType::Hash(hmap) = &entry.value {
                                let mut response = format!("*{}\r\n", hmap.len() * 2);
                                for (f, v) in hmap {
                                    response.push_str(&format!("${}\r\n{}\r\n${}\r\n{}\r\n", f.len(), f, v.len(), v));
                                }
                                let _ = stream.write_all(response.as_bytes()).await;
                            } else {
                                let _ = stream.write_all(b"-WRONGTYPE\r\n").await;
                            }
                        } else {
                            let _ = stream.write_all(b"*0\r\n").await;
                        }
                    }
            Command::HGet(key, field) => {
                        let map = db.read_shard(&key).await;
                        if let Some(entry) = map.get(&key) {
                            if let crate::engine::DataType::Hash(hmap) = &entry.value {
                                match hmap.get(&field) {
                                    Some(val) => {
                                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    }
                                    None => {
                                        let _ = stream.write_all(b"$-1\r\n").await;
                                    }
                                }
                            } else {
                                let _ = stream.write_all(b"-WRONGTYPE\r\n").await;
                            }
                        } else {
                            let _ = stream.write_all(b"$-1\r\n").await;
                        }
                    }
            Command::RPopLPush(source, destination) => {
                let mut popped_val = None;
                
                match db.write_multi_shards(&source, &destination).await {
                    MultiWriteGuard::Single(mut shard) => {
                        if let Some(entry) = shard.get_mut(&source) {
                            if let engine::DataType::List(list) = &mut entry.value {
                                popped_val = list.pop_back();
                            }
                        }

                        if let Some(val) = &popped_val {
                            let dest_entry = shard.entry(destination.clone()).or_insert_with(|| Entry {
                                value: engine::DataType::List(std::collections::VecDeque::new()),
                                expires_at: None,
                            });
                            if let engine::DataType::List(dest_list) = &mut dest_entry.value {
                                dest_list.push_front(val.clone());
                            }
                        }
                    }

                    MultiWriteGuard::Double(mut shard_src, mut shard_dest) => {
                        if let Some(entry) = shard_src.get_mut(&source) {
                            if let engine::DataType::List(list) = &mut entry.value {
                                popped_val = list.pop_back();
                            }
                        }

                        if let Some(val) = &popped_val {
                            let dest_entry = shard_dest.entry(destination.clone()).or_insert_with(|| Entry {
                                value: engine::DataType::List(std::collections::VecDeque::new()),
                                expires_at: None
                            });
                            if let engine::DataType::List(dest_list) =&mut dest_entry.value {
                                dest_list.push_front(val.clone());
                            }
                        }
                    }
                }

                match popped_val {
                    Some(val) => {
                        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("database.aof").await {
                            let log = format!("RPOPLPUSH {} {}\n", source, destination);
                            let _ = file.write_all(log.as_bytes()).await;
                        }
                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    None => {
                        let _ = stream.write_all(b"$-1\r\n").await;
                    }
                }
                // let popped_val = {
                //     let mut map = db.write_shard(&source).await;
                //     if let Some(entry) = map.get_mut(&source) {
                //         if let crate::engine::DataType::List(list) = &mut entry.value {
                //             list.pop_back()
                //         } else {
                //             None
                //         }
                //     } else {
                //         None
                //     }
                // };

                // match popped_val {
                //     Some(val) => {
                //         let mut map = db.write_shard(&destination).await;
                //         let dest_entry = map.entry(destination.clone()).or_insert_with(|| Entry {
                //             value: crate::engine::DataType::List(std::collections::VecDeque::new()),
                //             expires_at: None,
                //         });

                //         if let crate::engine::DataType::List(dest_list) = &mut dest_entry.value {
                //             dest_list.push_front(val.clone());

                //             let mut file = OpenOptions::new().create(true).append(true).open("database.aof").await.unwrap();
                //             let log = format!("RPOPLPUSH {} {}\n", source, destination);
                //             let _ = file.write_all(log.as_bytes()).await;

                //             let response = format!("${}\r\n{}\r\n", val.len(), val);
                //             let _ = stream.write_all(response.as_bytes()).await;
                //         } else {
                //             let _ = stream.write_all(b"-WRONGTYPE Destination is no a list\r\n").await;
                //         }
                //     }
                //     None => {
                //         let _ = stream.write_all(b"$-1\r\n").await;
                //     }
                // }
            }
            Command::LRem(key, _count, value_to_remove) => {
                let mut map = db.write_shard(&key).await;
                let mut removed = false;

                if let Some(entry) = map.get_mut(&key) {
                    if let crate::engine::DataType::List(list) = &mut entry.value {
                        if let Some(index) = list.iter().position(|x| x == &value_to_remove) {
                            list.remove(index);
                            removed = true;
                        }
                    }
                }

                if removed {
                    let mut file = OpenOptions::new().create(true).append(true).open("database.aof").await.unwrap();
                    let log = format!("LREM {} 1 \"{}\"\n", key, value_to_remove);
                    let _ = file.write_all(log.as_bytes()).await;

                    let _ = stream.write_all(b":1\r\n").await;
                } else {
                    let _ = stream.write_all(b":0\r\n").await;
                }
            }
            Command::MGet(key_a, key_b) => {
                let mut val_a = None;
                let mut val_b = None;

                match db.read_multi_shards(&key_a, &key_b).await {
                    MultiReadGuard::Single(shard) => {
                        if let Some(entry) = shard.get(&key_a) {
                            if let crate::engine::DataType::String(s) = &entry.value { val_a = Some(s.clone()); }
                        }
                        if let Some(entry) = shard.get(&key_b) {
                            if let crate::engine::DataType::String(s) = &entry.value { val_b = Some(s.clone()); }
                        }
                    }

                    MultiReadGuard::Double(shard_a, shard_b) => {
                        if let Some(entry) = shard_a.get(&key_a) {
                            if let crate::engine::DataType::String(s) = &entry.value { val_a = Some(s.clone()); }
                        }
                        if let Some(entry) = shard_b.get(&key_b) {
                            if let crate::engine::DataType::String(s) = &entry.value { val_b = Some(s.clone()); }
                        }
                    }
                }

                let mut response = format!("*2\r\n");

                for val in &[val_a, val_b] {
                    match val {
                        Some(s) => response.push_str(&format!("${}\r\n{}\r\n", s.len(), s)),
                        None => response.push_str("$-1\r\n"),
                    }
                }

                let _ = stream.write_all(response.as_bytes()).await;
             }
            Command::SAdd(key, member) => {
                let mut shard = db.write_shard(&key).await;

                let mut added = 0;

                let entry = shard.entry(key.clone()).or_insert_with(|| crate::engine::Entry {
                    value: crate::engine::DataType::Set(std::collections::HashSet::new()),
                    expires_at: None,
                });

                if let crate::engine::DataType::Set(set) = &mut entry.value {
                    if set.insert(member.clone()) {
                        added = 1;

                        if let Ok(mut file) = OpenOptions::new().create(true).open("database.aof").await {
                            let log = format!("SADD {} {}\n", key, member);
                            let _ = file.write_all(log.as_bytes()).await;
                        }
                    }
                }

                let _ = stream.write_all(format!(":{}\r\n", added).as_bytes()).await;
            }
            Command::SInter(key_a, key_b) => {
                let mut set_a = std::collections::HashSet::new();
                let mut set_b = std::collections::HashSet::new();

                match db.read_multi_shards(&key_a, &key_b).await {
                    MultiReadGuard::Single(shard) => {
                        if let Some(e) = shard.get(&key_a) { if let crate::engine::DataType::Set(s) = &e.value { set_a =s.clone(); } }
                        if let Some(e) = shard.get(&key_b) { if let crate::engine::DataType::Set(s) = &e.value{ set_b = s.clone(); } }
                    }
                    MultiReadGuard::Double(shard_a, shard_b) => {
                        if let Some(e) = shard_a.get(&key_a) { if let crate::engine::DataType::Set(s) = &e.value { set_a = s.clone(); } }
                        if let Some(e) = shard_b.get(&key_b) { if let crate::engine::DataType::Set(s) = &e.value { set_b = s.clone(); } }
                    }
                }

                let intersection: Vec<&String> = set_a.iter().filter(|item| set_b.contains(*item)).collect();

                let mut response = format!("*{}\r\n", intersection.len());

                for item in intersection {
                    response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
                }

                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Unknown => {
                if let Err(e) = stream.write_all(b"-ERR unknown command\r\n").await {
                    crate::log_error!("Server", "Failed to write to client: {}", e);
                    break;
                }
            }
        }
    }
}

pub async fn run(address: &str, db: Db, pubsub: PubSub) {
    let listener = TcpListener::bind(address).await.expect("Could not bind to address");
    crate::log_success!("Server", "Titan KV natively deployed and listening on {}", address);

    loop {
        match listener.accept().await {
            Ok((stream, socket_addr)) => {
                crate::log_info!("Server", "New connection from {}", socket_addr);
                let db_handle = db.clone();
                let pubsub_handle = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    handle_connection(stream, db_handle, pubsub_handle).await;
                });
            }
            Err(e) => {
                crate::log_error!("Server", "Connection Failed: {}", e);
            }
        };
    }
}
