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
use std::collections::HashMap;
// use tokio::fs::OpenOptions;
use std::sync::{Arc, Mutex};
use std::time::{ SystemTime, UNIX_EPOCH };
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

struct ClientInfo {
    id: u64,
    addr: SocketAddr,
    connected_at: SystemTime,
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

struct ClientGuard {
    addr: SocketAddr,
    clients: Arc<Mutex<HashMap<SocketAddr, ClientInfo>>>,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        if let Ok(mut clients) = self.clients.lock() {
            clients.remove(&self.addr);
        }
    }
}

// async fn append_aof(log: String) {
//     if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("database.aof").await {
//         let _ = file.write_all(log.as_bytes()).await;
//     } else {
//         crate::log_error!("AOF", "Failed to append to AOF file (Access Denied/Locked)");
//     }
// }

use crate::engine::{ self, Db, Entry, MultiWriteGuard, MultiReadGuard };
use crate::protocol::{ parse_command, Command, read_resp };
// use crate::thread_pool::ThreadPool;

async fn handle_connection(
    stream: TcpStream, 
    db: Db, 
    pubsub: PubSub, 
    socket_addr: SocketAddr,
    active_clients: Arc<Mutex<HashMap<SocketAddr, ClientInfo>>>
) {
    let _guard = ClientGuard {
        addr: socket_addr,
        clients: active_clients.clone(),
    };

    let (read_half, mut stream) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        // NEW: Uses our dedicated RESP reader
        let parts: Vec<String> = match read_resp(&mut reader).await {
            Ok(p) if !p.is_empty() => p,
            _ => {
                crate::log_info!("Client", "Client Disconnected.");
                break;
            }
        };

        let command = parse_command(&parts);

        let summary_parts: Vec<String> = parts
            .iter()
            .map(|p| {
                if p.len() > 30 { format!("{}...({}b)", &p[..15], p.len()) } else { p.clone() }
            })
            .collect();
        crate::log_info!("Command", "{}", summary_parts.join(" "));

        if !parts.is_empty() && parts[0].to_uppercase() != "MONITOR" {
            if let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) {
                let timestamp = duration.as_secs_f64();

                let cmd_string = parts
                    .iter()
                    .map(|p| format!("\"{}\"", p))
                    .collect::<Vec<String>>()
                    .join(" ");

                let log_msg = format!("{} [0 {}] {}", timestamp, socket_addr, cmd_string);

                let _ = db.tx.send(log_msg);
            }
        }

        match command {
            Command::ClientList => {
                let mut buf = String::new();
                let now = SystemTime::now();

                {
                    let clients = active_clients.lock().unwrap();

                    for info in clients.values() {
                        let age = now.duration_since(info.connected_at).unwrap_or_default().as_secs();

                        buf.push_str(&format!(
                            "id={} addr={} fd=-1 name= age={} idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n",
                            info.id, info.addr, age
                        ));
                    }
                }

                let response = format!("${}\r\n{}\r\n", buf.len(), buf);
                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Strlen(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::String(val) => format!(":{}\r\n", val.len()),
                        _ => "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string(),
                    },
                    None => ":0\r\n".to_string(),
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::DbSize => {
                let mut total_keys = 0;
                for i in 0..db.get_shard_count() {
                    let map = db.read_shard_by_index(i).await;
                    total_keys += map.len();
                }
                let response = format!(":{}\r\n", total_keys);
                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::SetEx(key, seconds, value) => {
                let expiration_time = SystemTime::now() + Duration::from_secs(seconds as u64);

                let new_entry = Entry {
                    value: crate::engine::DataType::String(value.clone()),
                    expires_at: Some(expiration_time),
                };

                let mut map = db.write_shard(&key).await;
                map.insert(key.clone(), new_entry);

                // OPTIMIZED: Drop the log into our memory channel. It takes nanoseconds!
                let log = format!("SETEX {} {} {}\n", key, seconds, value);
                let _ = db.aof_tx.send(log).await; 
                
                let _ = stream.write_all(b"+OK\r\n").await;
            }
            Command::Ping => {
                let _ = stream.write_all(b"+PONG\r\n").await;
            }
            Command::Set(key, value) => {
                let mut shard = db.write_shard(&key).await;

                shard.insert(key.clone(), Entry {
                    value: engine::DataType::String(value.clone()),
                    expires_at: None,
                });

                // OPTIMIZED: Added missing AOF persistence for standard SET commands!
                let log = format!("SET {} {}\n", key, value);
                let _ = db.aof_tx.send(log).await;

                let _ = stream.write_all(b"+OK\r\n").await;
            }
            Command::Get(key) => {
                let mut map = db.write_shard(&key).await;

                match map.get(&key) {
                    Some(entry) => {
                        if let Some(expiration) = entry.expires_at {
                            if SystemTime::now() > expiration {
                                if map.remove(&key).is_some() {
                                    // OPTIMIZED: Async channel log for passive expiration delete
                                    let log = format!("DEL {}\n", key);
                                    let _ = db.aof_tx.send(log).await;

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
                    // OPTIMIZED: Async channel log for explicit deletes
                    let log = format!("DEL {}\n", key);
                    let _ = db.aof_tx.send(log).await;
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
                                        if let Err(e) = stream.write_all(
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

                // OPTIMIZED: Async channel log for value increments
                let log = format!("INCR {}\n", key);
                let _ = db.aof_tx.send(log).await;

                let response = format!(":{}\r\n", new_num);
                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Publish(channel, message) => {
                crate::pubsub::handle_publish(&pubsub, &channel, &message, &mut stream).await;
            }
            Command::Subscribe(channel) => {
                crate::pubsub::handle_subscribe(&pubsub, &channel, &mut stream, &mut reader).await;
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

                        // OPTIMIZED: Shifting LPush logs to the async channel memory buffer
                        let log = format!("LPUSH {} \"{}\"\n", key, value);
                        let _ = db.aof_tx.send(log).await;

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
                                // OPTIMIZED: Shifting LPop logs to the async channel memory buffer
                                let log = format!("LPOP {}\n", key);
                                let _ = db.aof_tx.send(log).await;

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
                                // OPTIMIZED: Shifting RPop logs to the async channel memory buffer
                                let log = format!("RPOP {}\n", key);
                                let _ = db.aof_tx.send(log).await;

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

                                while (list.len() as i32) > e + 1 {
                                    list.pop_back();
                                }
                                for _ in 0..s {
                                    list.pop_front();
                                }

                                if list.is_empty() {
                                    map.remove(&key);
                                }
                            }

                            // OPTIMIZED: Shifting LTrim logs to the async channel memory buffer
                            let log = format!("LTRIM {} {} {}\n", key, start, stop);
                            let _ = db.aof_tx.send(log).await;

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
                                        response.push_str(
                                            &format!("${}\r\n{}\r\n", val.len(), val)
                                        );
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

                    // OPTIMIZED: Shifting RPush logs to the async channel memory buffer
                    let log = format!("RPUSH {} \"{}\"\n", key, value);
                    let _ = db.aof_tx.send(log).await;

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
                    
                    // OPTIMIZED: Shifting HSet logs to the async channel memory buffer
                    let log = format!("HSET {} {} \"{}\"\n", key, field, value);
                    let _ = db.aof_tx.send(log).await;

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
                            response.push_str(
                                &format!("${}\r\n{}\r\n${}\r\n{}\r\n", f.len(), f, v.len(), v)
                            );
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
                            let dest_entry = shard
                                .entry(destination.clone())
                                .or_insert_with(|| Entry {
                                    value: engine::DataType::List(
                                        std::collections::VecDeque::new()
                                    ),
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
                            let dest_entry = shard_dest
                                .entry(destination.clone())
                                .or_insert_with(|| Entry {
                                    value: engine::DataType::List(
                                        std::collections::VecDeque::new()
                                    ),
                                    expires_at: None,
                                });
                            if let engine::DataType::List(dest_list) = &mut dest_entry.value {
                                dest_list.push_front(val.clone());
                            }
                        }
                    }
                }

                match popped_val {
                    Some(val) => {
                        // OPTIMIZED: Removed OpenOptions. Send log safely via memory channel.
                        let log = format!("RPOPLPUSH {} {}\n", source, destination);
                        let _ = db.aof_tx.send(log).await;
                        
                        let response = format!("${}\r\n{}\r\n", val.len(), val);
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    None => {
                        let _ = stream.write_all(b"$-1\r\n").await;
                    }
                }
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
                    // OPTIMIZED: No more file descriptor unpacking or unwrap() panics here!
                    let log = format!("LREM {} 1 \"{}\"\n", key, value_to_remove);
                    let _ = db.aof_tx.send(log).await;

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
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_a = Some(s.clone());
                            }
                        }
                        if let Some(entry) = shard.get(&key_b) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_b = Some(s.clone());
                            }
                        }
                    }

                    MultiReadGuard::Double(shard_a, shard_b) => {
                        if let Some(entry) = shard_a.get(&key_a) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_a = Some(s.clone());
                            }
                        }
                        if let Some(entry) = shard_b.get(&key_b) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_b = Some(s.clone());
                            }
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

                        // OPTIMIZED: Shifted blocking raw file write out to async writer channel
                        let log = format!("SADD {} {}\n", key, member);
                        let _ = db.aof_tx.send(log).await;
                    }
                }

                let _ = stream.write_all(format!(":{}\r\n", added).as_bytes()).await;
            }
            Command::SInter(key_a, key_b) => {
                let mut set_a = std::collections::HashSet::new();
                let mut set_b = std::collections::HashSet::new();

                match db.read_multi_shards(&key_a, &key_b).await {
                    MultiReadGuard::Single(shard) => {
                        if let Some(e) = shard.get(&key_a) {
                            if let crate::engine::DataType::Set(s) = &e.value {
                                set_a = s.clone();
                            }
                        }
                        if let Some(e) = shard.get(&key_b) {
                            if let crate::engine::DataType::Set(s) = &e.value {
                                set_b = s.clone();
                            }
                        }
                    }
                    MultiReadGuard::Double(shard_a, shard_b) => {
                        if let Some(e) = shard_a.get(&key_a) {
                            if let crate::engine::DataType::Set(s) = &e.value {
                                set_a = s.clone();
                            }
                        }
                        if let Some(e) = shard_b.get(&key_b) {
                            if let crate::engine::DataType::Set(s) = &e.value {
                                set_b = s.clone();
                            }
                        }
                    }
                }

                let intersection: Vec<&String> = set_a
                    .iter()
                    .filter(|item| set_b.contains(*item))
                    .collect();

                let mut response = format!("*{}\r\n", intersection.len());

                for item in intersection {
                    response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
                }

                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Keys(pattern) => {
                let regex_string = format!("^{}$", pattern.replace("*", ".*").replace("?", "."));

                let matcher = match regex::Regex::new(&regex_string) {
                    Ok(re) => re,
                    Err(_) => {
                        let _ = stream.write_all(b"-ERR invalid pattern format\r\n").await;
                        return;
                    }
                };

                let all_keys = db.get_all_keys().await;

                let filtered_keys: Vec<String> = all_keys
                    .into_iter()
                    .filter(|key| matcher.is_match(key))
                    .collect();

                let mut response = format!("*{}\r\n", filtered_keys.len());

                for key in filtered_keys {
                    response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                }

                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Scan(cursor, match_pattern) => {
                if cursor >= 64 {
                    let _ = stream.write_all(b"*2\r\n$1\r\n0\r\n*0\r\n").await;
                    return;
                }

                let mut keys = db.scan_shard(cursor).await;

                if let Some(pattern) = match_pattern {
                    let regex_string = format!(
                        "^{}$",
                        pattern.replace("*", ".*").replace("?", ".")
                    );
                    if let Ok(matcher) = regex::Regex::new(&regex_string) {
                        keys.retain(|key| matcher.is_match(key));
                    }
                }

                let next_cursor = if cursor == 63 { 0 } else { cursor + 1 };
                let next_cursor_str = next_cursor.to_string();

                let mut response = format!(
                    "*2\r\n${}\r\n{}\r\n*{}\r\n",
                    next_cursor_str.len(),
                    next_cursor_str,
                    keys.len()
                );

                for key in keys {
                    response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                }

                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Monitor => {
                let _ = stream.write_all(b"+OK\r\n").await;

                let mut rx = db.tx.subscribe();

                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            let response = format!("+{}\r\n", msg);
                            if stream.write_all(response.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            continue
                        }
                        Err(_) => break,
                    }
                }
            }
            Command::Info => {
                let info_reply = "# Server\r\ntitan_version:2.0.0\r\nos:Linux\r\n";
                let response = format!("${}\r\n{}\r\n", info_reply.len(), info_reply);
                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Client => {
                let _ = stream.write_all(b"$-1\r\n").await;
            }
            Command::Ttl(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match entry.expires_at {
                        Some(expiration) => {
                            if let Ok(duration) = expiration.duration_since(std::time::SystemTime::now()) {
                                format!(":{}\r\n", duration.as_secs())
                            } else {
                                ":-2\r\n".to_string() // Already expired
                            }
                        }
                        None => ":-1\r\n".to_string(), // Key exists but has no associated expire
                    },
                    None => ":-2\r\n".to_string(), // Key does not exist
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::Type(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::String(_) => "+string\r\n",
                        crate::engine::DataType::List(_) => "+list\r\n",
                        crate::engine::DataType::Hash(_) => "+hash\r\n",
                        crate::engine::DataType::Set(_) => "+set\r\n",
                    },
                    None => "+none\r\n",
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::Pttl(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match entry.expires_at {
                        Some(expiration) => {
                            if let Ok(duration) = expiration.duration_since(SystemTime::now()) {
                                format!(":{}\r\n", duration.as_millis()) // Return MS precision TTL
                            } else {
                                ":-2\r\n".to_string() // Expired
                            }
                        }
                        None => ":-1\r\n".to_string(), // No expiration set
                    },
                    None => ":-2\r\n".to_string(), // Key doesn't exist
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::Memory => {
                // Tiny RDM uses MEMORY USAGE to show bytes. Let's return a stable mock size
                let _ = stream.write_all(b":64\r\n").await;
            }
            Command::Llen(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::List(list) => format!(":{}\r\n", list.len()),
                        _ => "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string(),
                    },
                    None => ":0\r\n".to_string(),
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::Hlen(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::Hash(hmap) => format!(":{}\r\n", hmap.len()),
                        _ => "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string(),
                    },
                    None => ":0\r\n".to_string(),
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::Scard(key) => {
                let map = db.read_shard(&key).await;
                let reply = match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::Set(set) => format!(":{}\r\n", set.len()),
                        _ => "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string(),
                    },
                    None => ":0\r\n".to_string(),
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::SMembers(key) => {
                let map = db.read_shard(&key).await;
                match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::Set(set) => {
                            let mut response = format!("*{}\r\n", set.len());
                            for member in set {
                                response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                            }
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                        _ => {
                            let _ = stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").await;
                        }
                    },
                    None => {
                        let _ = stream.write_all(b"*0\r\n").await;
                    }
                }
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

    // Dynamic central state registry tracking active connections across thread pools
    let active_clients = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, socket_addr)) => {
                crate::log_info!("Server", "New connection from {}", socket_addr);
                    
                let db_handle = db.clone();
                let pubsub_handle = Arc::clone(&pubsub);
                let clients_handle = active_clients.clone();

                    // Generate tracking meta context and save it prior to worker spawning
                let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                {
                    let mut clients = active_clients.lock().unwrap();
                    clients.insert(socket_addr, ClientInfo {
                        id: client_id,
                        addr: socket_addr,
                        connected_at: SystemTime::now(),
                    });
                }

                tokio::spawn(async move {
                    handle_connection(stream, db_handle, pubsub_handle, socket_addr, clients_handle).await;
                });
            }
            Err(e) => {
                crate::log_error!("Server", "Connection Failed: {}", e);
            }
        };
    }
}
