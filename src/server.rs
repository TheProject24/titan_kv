// src/server.rs

use crate::pubsub::{ PubSub };
use std::io::Write as IoWrite;
use std::time::Duration;
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{
    AsyncWriteExt,
    BufReader,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{ SystemTime, UNIX_EPOCH };
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;

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

use crate::engine::{ self, Db, Entry, MultiWriteGuard, MultiReadGuard };
use crate::protocol::{ parse_command, Command, read_resp };

// Build a RESP bulk string into a single Vec<u8> so we do one write() syscall per response.
// The header is a small format!(), the data is copied from stored Bytes — one allocation, one
// syscall (same as the original format! path but now source data can be any &[u8]).
macro_rules! write_bulk {
    ($stream:expr, $val:expr) => {{
        let header = format!("${}\r\n", $val.len());
        let mut buf = Vec::with_capacity(header.len() + $val.len() + 2);
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(&$val[..]);
        buf.extend_from_slice(b"\r\n");
        let _ = $stream.write_all(&buf).await;
    }};
}

fn bytes_to_str(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

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
        let parts: Vec<Bytes> = match read_resp(&mut reader).await {
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
                let s = bytes_to_str(p);
                if s.len() > 30 { format!("{}...({}b)", &s[..15], s.len()) } else { s.to_string() }
            })
            .collect();
        crate::log_info!("Command", "{}", summary_parts.join(" "));

        if !parts.is_empty() && !parts[0].eq_ignore_ascii_case(b"MONITOR") {
            if let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) {
                let timestamp = duration.as_secs_f64();

                let cmd_string = parts
                    .iter()
                    .map(|p| format!("\"{}\"", bytes_to_str(p)))
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
                    value: crate::engine::DataType::String(value.clone()), // O(1)
                    expires_at: Some(expiration_time),
                };

                let mut map = db.write_shard(&key).await;
                map.insert(key.clone(), new_entry); // O(1)

                let log = format!("SETEX {} {} {}\n", bytes_to_str(&key), seconds, bytes_to_str(&value));
                let _ = db.aof_tx.send(log).await;

                let _ = stream.write_all(b"+OK\r\n").await;
            }
            Command::Ping => {
                let _ = stream.write_all(b"+PONG\r\n").await;
            }
            Command::Set(key, value) => {
                let mut shard = db.write_shard(&key).await;

                shard.insert(key.clone(), Entry { // O(1) clone
                    value: engine::DataType::String(value.clone()), // O(1) clone
                    expires_at: None,
                });

                let log = format!("SET {} {}\n", bytes_to_str(&key), bytes_to_str(&value));
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
                                    let log = format!("DEL {}\n", bytes_to_str(&key));
                                    let _ = db.aof_tx.send(log).await;
                                    let _ = stream.write_all(b"$-1\r\n").await;
                                }
                                continue;
                            }
                        }
                        match &entry.value {
                            crate::engine::DataType::String(val) => {
                                // Zero-copy: write header then stored bytes directly
                                write_bulk!(stream, val);
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
                    let log = format!("DEL {}\n", bytes_to_str(&key));
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
                                match std::str::from_utf8(val).ok().and_then(|s| s.parse::<i64>().ok()) {
                                    Some(num) => num,
                                    None => {
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
                    // Bytes::from(String) is zero-copy — takes ownership of the String buffer
                    value: crate::engine::DataType::String(Bytes::from(new_num.to_string())),
                    expires_at: None,
                };
                map.insert(key.clone(), new_entry); // O(1)

                let log = format!("INCR {}\n", bytes_to_str(&key));
                let _ = db.aof_tx.send(log).await;

                let response = format!(":{}\r\n", new_num);
                let _ = stream.write_all(response.as_bytes()).await;
            }
            Command::Publish(channel, message) => {
                crate::pubsub::handle_publish(&pubsub, bytes_to_str(&channel), bytes_to_str(&message), &mut stream).await;
            }
            Command::Subscribe(channel) => {
                crate::pubsub::handle_subscribe(&pubsub, bytes_to_str(&channel), &mut stream, &mut reader).await;
                break;
            }
            Command::Unsubscribe(channel) => {
                let ch = bytes_to_str(&channel);
                let ack = format!(
                    "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:0\r\n",
                    channel.len(),
                    ch
                );
                let _ = stream.write_all(ack.as_bytes()).await;
            }
            Command::LPush(key, value) => {
                let mut map = db.write_shard(&key).await;

                let entry = map.entry(key.clone()).or_insert_with(|| Entry { // O(1)
                    value: crate::engine::DataType::List(std::collections::VecDeque::new()),
                    expires_at: None,
                });

                match &mut entry.value {
                    crate::engine::DataType::List(list) => {
                        list.push_front(value.clone()); // O(1)
                        let len = list.len();

                        let log = format!("LPUSH {} \"{}\"\n", bytes_to_str(&key), bytes_to_str(&value));
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
                                let log = format!("LPOP {}\n", bytes_to_str(&key));
                                let _ = db.aof_tx.send(log).await;
                                write_bulk!(stream, val);
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
                                let log = format!("RPOP {}\n", bytes_to_str(&key));
                                let _ = db.aof_tx.send(log).await;
                                write_bulk!(stream, val);
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

                            let log = format!("LTRIM {} {} {}\n", bytes_to_str(&key), start, stop);
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

                                // Build response into a Vec<u8> — no intermediate String per element
                                let count = e - s + 1;
                                let mut response = Vec::with_capacity(count * 32);
                                write!(response, "*{}\r\n", count).unwrap();
                                for i in s..=e {
                                    if let Some(val) = list.get(i) {
                                        write!(response, "${}\r\n", val.len()).unwrap();
                                        response.extend_from_slice(val);
                                        response.extend_from_slice(b"\r\n");
                                    }
                                }
                                let _ = stream.write_all(&response).await;
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
                let entry = map.entry(key.clone()).or_insert_with(|| Entry { // O(1)
                    value: crate::engine::DataType::List(std::collections::VecDeque::new()),
                    expires_at: None,
                });

                if let crate::engine::DataType::List(list) = &mut entry.value {
                    list.push_back(value.clone()); // O(1)
                    let len = list.len();

                    let log = format!("RPUSH {} \"{}\"\n", bytes_to_str(&key), bytes_to_str(&value));
                    let _ = db.aof_tx.send(log).await;

                    let response = format!(":{}\r\n", len);
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
            Command::HSet(key, field, value) => {
                let mut map = db.write_shard(&key).await;
                let entry = map.entry(key.clone()).or_insert_with(|| Entry { // O(1)
                    value: crate::engine::DataType::Hash(std::collections::HashMap::new()),
                    expires_at: None,
                });
                if let crate::engine::DataType::Hash(hmap) = &mut entry.value {
                    hmap.insert(field.clone(), value.clone()); // O(1) each

                    let log = format!("HSET {} {} \"{}\"\n",
                        bytes_to_str(&key), bytes_to_str(&field), bytes_to_str(&value));
                    let _ = db.aof_tx.send(log).await;

                    let _ = stream.write_all(b"+OK\r\n").await;
                } else {
                    let _ = stream.write_all(b"-WRONGTYPE\r\n").await;
                }
            }
            Command::HGetAll(key) => {
                let map = db.read_shard(&key).await;
                if let Some(entry) = map.get(&key) {
                    if let crate::engine::DataType::Hash(hmap) = &entry.value {
                        let mut response = Vec::with_capacity(hmap.len() * 64);
                        write!(response, "*{}\r\n", hmap.len() * 2).unwrap();
                        for (f, v) in hmap {
                            write!(response, "${}\r\n", f.len()).unwrap();
                            response.extend_from_slice(f);
                            response.extend_from_slice(b"\r\n");
                            write!(response, "${}\r\n", v.len()).unwrap();
                            response.extend_from_slice(v);
                            response.extend_from_slice(b"\r\n");
                        }
                        let _ = stream.write_all(&response).await;
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
                                write_bulk!(stream, val);
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
                                .entry(destination.clone()) // O(1)
                                .or_insert_with(|| Entry {
                                    value: engine::DataType::List(
                                        std::collections::VecDeque::new()
                                    ),
                                    expires_at: None,
                                });
                            if let engine::DataType::List(dest_list) = &mut dest_entry.value {
                                dest_list.push_front(val.clone()); // O(1)
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
                                .entry(destination.clone()) // O(1)
                                .or_insert_with(|| Entry {
                                    value: engine::DataType::List(
                                        std::collections::VecDeque::new()
                                    ),
                                    expires_at: None,
                                });
                            if let engine::DataType::List(dest_list) = &mut dest_entry.value {
                                dest_list.push_front(val.clone()); // O(1)
                            }
                        }
                    }
                }

                match popped_val {
                    Some(val) => {
                        let log = format!("RPOPLPUSH {} {}\n",
                            bytes_to_str(&source), bytes_to_str(&destination));
                        let _ = db.aof_tx.send(log).await;
                        write_bulk!(stream, val);
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
                    let log = format!("LREM {} 1 \"{}\"\n",
                        bytes_to_str(&key), bytes_to_str(&value_to_remove));
                    let _ = db.aof_tx.send(log).await;
                    let _ = stream.write_all(b":1\r\n").await;
                } else {
                    let _ = stream.write_all(b":0\r\n").await;
                }
            }
            Command::MGet(key_a, key_b) => {
                let mut val_a: Option<Bytes> = None;
                let mut val_b: Option<Bytes> = None;

                match db.read_multi_shards(&key_a, &key_b).await {
                    MultiReadGuard::Single(shard) => {
                        if let Some(entry) = shard.get(&key_a) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_a = Some(s.clone()); // O(1)
                            }
                        }
                        if let Some(entry) = shard.get(&key_b) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_b = Some(s.clone()); // O(1)
                            }
                        }
                    }

                    MultiReadGuard::Double(shard_a, shard_b) => {
                        if let Some(entry) = shard_a.get(&key_a) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_a = Some(s.clone()); // O(1)
                            }
                        }
                        if let Some(entry) = shard_b.get(&key_b) {
                            if let crate::engine::DataType::String(s) = &entry.value {
                                val_b = Some(s.clone()); // O(1)
                            }
                        }
                    }
                }

                let mut response = Vec::with_capacity(64);
                response.extend_from_slice(b"*2\r\n");
                for val in &[val_a, val_b] {
                    match val {
                        Some(s) => {
                            write!(response, "${}\r\n", s.len()).unwrap();
                            response.extend_from_slice(s);
                            response.extend_from_slice(b"\r\n");
                        }
                        None => response.extend_from_slice(b"$-1\r\n"),
                    }
                }
                let _ = stream.write_all(&response).await;
            }
            Command::SAdd(key, member) => {
                let mut shard = db.write_shard(&key).await;
                let mut added = 0;

                let entry = shard.entry(key.clone()).or_insert_with(|| crate::engine::Entry { // O(1)
                    value: crate::engine::DataType::Set(std::collections::HashSet::new()),
                    expires_at: None,
                });

                if let crate::engine::DataType::Set(set) = &mut entry.value {
                    if set.insert(member.clone()) { // O(1)
                        added = 1;
                        let log = format!("SADD {} {}\n", bytes_to_str(&key), bytes_to_str(&member));
                        let _ = db.aof_tx.send(log).await;
                    }
                }

                let _ = stream.write_all(format!(":{}\r\n", added).as_bytes()).await;
            }
            Command::SInter(key_a, key_b) => {
                let mut set_a = std::collections::HashSet::<Bytes>::new();
                let mut set_b = std::collections::HashSet::<Bytes>::new();

                match db.read_multi_shards(&key_a, &key_b).await {
                    MultiReadGuard::Single(shard) => {
                        if let Some(e) = shard.get(&key_a) {
                            if let crate::engine::DataType::Set(s) = &e.value {
                                set_a = s.clone(); // HashSet clone — each Bytes element is O(1)
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

                let intersection: Vec<&Bytes> = set_a
                    .iter()
                    .filter(|item| set_b.contains(*item))
                    .collect();

                let mut response = Vec::with_capacity(intersection.len() * 32);
                write!(response, "*{}\r\n", intersection.len()).unwrap();
                for item in intersection {
                    write!(response, "${}\r\n", item.len()).unwrap();
                    response.extend_from_slice(item);
                    response.extend_from_slice(b"\r\n");
                }
                let _ = stream.write_all(&response).await;
            }
            Command::Keys(pattern) => {
                let pattern_str = bytes_to_str(&pattern);
                let regex_string = format!("^{}$", pattern_str.replace("*", ".*").replace("?", "."));

                let matcher = match regex::Regex::new(&regex_string) {
                    Ok(re) => re,
                    Err(_) => {
                        let _ = stream.write_all(b"-ERR invalid pattern format\r\n").await;
                        return;
                    }
                };

                let all_keys = db.get_all_keys().await;

                let filtered_keys: Vec<Bytes> = all_keys
                    .into_iter()
                    .filter(|key| {
                        std::str::from_utf8(key).map(|s| matcher.is_match(s)).unwrap_or(false)
                    })
                    .collect();

                let mut response = Vec::with_capacity(filtered_keys.len() * 32);
                write!(response, "*{}\r\n", filtered_keys.len()).unwrap();
                for key in &filtered_keys {
                    write!(response, "${}\r\n", key.len()).unwrap();
                    response.extend_from_slice(key);
                    response.extend_from_slice(b"\r\n");
                }
                let _ = stream.write_all(&response).await;
            }
            Command::Scan(cursor, match_pattern) => {
                if cursor >= 64 {
                    let _ = stream.write_all(b"*2\r\n$1\r\n0\r\n*0\r\n").await;
                    return;
                }

                let mut keys = db.scan_shard(cursor).await;

                if let Some(pattern) = match_pattern {
                    let pattern_str = bytes_to_str(&pattern);
                    let regex_string = format!(
                        "^{}$",
                        pattern_str.replace("*", ".*").replace("?", ".")
                    );
                    if let Ok(matcher) = regex::Regex::new(&regex_string) {
                        keys.retain(|key| {
                            std::str::from_utf8(key).map(|s| matcher.is_match(s)).unwrap_or(false)
                        });
                    }
                }

                let next_cursor = if cursor == 63 { 0 } else { cursor + 1 };
                let next_cursor_str = next_cursor.to_string();

                let mut response = Vec::with_capacity(keys.len() * 32 + 32);
                write!(response, "*2\r\n${}\r\n{}\r\n*{}\r\n",
                    next_cursor_str.len(), next_cursor_str, keys.len()).unwrap();
                for key in &keys {
                    write!(response, "${}\r\n", key.len()).unwrap();
                    response.extend_from_slice(key);
                    response.extend_from_slice(b"\r\n");
                }
                let _ = stream.write_all(&response).await;
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
                                ":-2\r\n".to_string()
                            }
                        }
                        None => ":-1\r\n".to_string(),
                    },
                    None => ":-2\r\n".to_string(),
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
                                format!(":{}\r\n", duration.as_millis())
                            } else {
                                ":-2\r\n".to_string()
                            }
                        }
                        None => ":-1\r\n".to_string(),
                    },
                    None => ":-2\r\n".to_string(),
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            }
            Command::Memory => {
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
                            let mut response = Vec::with_capacity(set.len() * 32);
                            write!(response, "*{}\r\n", set.len()).unwrap();
                            for member in set {
                                write!(response, "${}\r\n", member.len()).unwrap();
                                response.extend_from_slice(member);
                                response.extend_from_slice(b"\r\n");
                            }
                            let _ = stream.write_all(&response).await;
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

    let active_clients = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, socket_addr)) => {
                crate::log_info!("Server", "New connection from {}", socket_addr);

                let db_handle = db.clone();
                let pubsub_handle = Arc::clone(&pubsub);
                let clients_handle = active_clients.clone();

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
