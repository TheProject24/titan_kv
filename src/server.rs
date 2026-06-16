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
use crate::config::{self, Config};
use subtle::ConstantTimeEq;

#[derive(PartialEq)]
enum ConnectionState {
    Unauthorized,
    Authenticated,
}

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

macro_rules! write_bulk {
    ($buffer:expr, $val:expr) => {{
        let header = format!("${}\r\n", $val.len());
        $buffer.extend_from_slice(header.as_bytes());
        $buffer.extend_from_slice(&$val[..]);
        $buffer.extend_from_slice(b"\r\n");
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
    active_clients: Arc<Mutex<HashMap<SocketAddr, ClientInfo>>>,
    requirepass: Arc<Option<String>>
) {
    let _guard = ClientGuard {
        addr: socket_addr,
        clients: active_clients.clone(),
    };

    let (read_half, mut stream) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let mut state = if requirepass.is_some() {
        ConnectionState::Unauthorized
    } else {
        ConnectionState::Authenticated
    };

    let mut write_buffer = Vec::with_capacity(8192);

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

        if state == ConnectionState::Unauthorized {
            match command {
                Command::Auth(provided_password) => {
                    let actual_password = requirepass.as_ref().unwrap();

                    // Security: Constant-time comparison
                    if provided_password.as_ref().ct_eq(actual_password.as_bytes()).into() {
                        state = ConnectionState::Authenticated;
                        write_buffer.extend_from_slice(b"+OK\r\n");
                    } else {
                        write_buffer.extend_from_slice(b"-ERR invalid password\r\n");
                    }
                }
                _ => {
                    // Reject any database operation if not authenticated
                    write_buffer.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
                }
            }

            // Flush the rejection/auth message immediately and wait for next command
            let _ = stream.write_all(&write_buffer).await;
            write_buffer.clear();
            continue; 
        }

        // 4. NORMAL EXECUTION BLOCK
        match command {
            Command::Auth(_) => {
                write_buffer.extend_from_slice(b"+OK\r\n");
            }
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
                write!(&mut write_buffer, "${}\r\n{}\r\n", buf.len(), buf).unwrap();
            }
            Command::Strlen(key) => {
                let map = db.read_shard(&key).await;
                match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::String(val) => {
                            write!(&mut write_buffer, ":{}\r\n", val.len()).unwrap();
                        }
                        _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    },
                    None => write_buffer.extend_from_slice(b":0\r\n"),
                };
            }
            Command::DbSize => {
                let mut total_keys = 0;
                for i in 0..db.get_shard_count() {
                    let map = db.read_shard_by_index(i).await;
                    total_keys += map.len();
                }
                write!(&mut write_buffer, ":{}\r\n", total_keys).unwrap();
            }
            Command::SetEx(key, seconds, value) => {
                let expiration_time = SystemTime::now() + Duration::from_secs(seconds as u64);

                let new_entry = Entry {
                    value: crate::engine::DataType::String(value.clone()),
                    expires_at: Some(expiration_time),
                };

                let mut map = db.write_shard(&key).await;
                map.insert(key.clone(), new_entry);

                let log = format!("SETEX {} {} {}\n", bytes_to_str(&key), seconds, bytes_to_str(&value));
                let _ = db.aof_tx.send(log).await;

                write_buffer.extend_from_slice(b"+OK\r\n");
            }
            Command::Ping => {
                write_buffer.extend_from_slice(b"+PONG\r\n");
            }
            Command::Set(key, value) => {
                let mut shard = db.write_shard(&key).await;

                shard.insert(key.clone(), Entry {
                    value: engine::DataType::String(value.clone()),
                    expires_at: None,
                });

                let log = format!("SET {} {}\n", bytes_to_str(&key), bytes_to_str(&value));
                let _ = db.aof_tx.send(log).await;

                write_buffer.extend_from_slice(b"+OK\r\n");
            }
            Command::Get(key) => {
                let mut map = db.write_shard(&key).await;
                let mut expired = false;

                if let Some(entry) = map.get(&key) {
                    if let Some(expiration) = entry.expires_at {
                        if SystemTime::now() > expiration {
                            expired = true;
                        }
                    }
                }

                if expired {
                    if map.remove(&key).is_some() {
                        let log = format!("DEL {}\n", bytes_to_str(&key));
                        let _ = db.aof_tx.send(log).await;
                    }
                    write_buffer.extend_from_slice(b"$-1\r\n");
                } else {
                    match map.get(&key) {
                        Some(entry) => match &entry.value {
                            crate::engine::DataType::String(val) => {
                                write_bulk!(write_buffer, val);
                            }
                            _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong type of value\r\n"),
                        },
                        None => write_buffer.extend_from_slice(b"$-1\r\n"),
                    }
                }
            }
            Command::Del(key) => {
                let mut map = db.write_shard(&key).await;
                if map.remove(&key).is_some() {
                    let log = format!("DEL {}\n", bytes_to_str(&key));
                    let _ = db.aof_tx.send(log).await;
                    write_buffer.extend_from_slice(b":1\r\n");
                } else {
                    write_buffer.extend_from_slice(b":0\r\n");
                }
            }
            Command::Exists(key) => {
                let map = db.read_shard(&key).await;
                if map.contains_key(&key) {
                    write_buffer.extend_from_slice(b":1\r\n");
                } else {
                    write_buffer.extend_from_slice(b":0\r\n");
                }
            }
            Command::Incr(key) => {
                let mut map = db.write_shard(&key).await;
                let mut error = None;
                
                let current_number = match map.get(&key) {
                    Some(entry) => match &entry.value {
                        crate::engine::DataType::String(val) => {
                            match std::str::from_utf8(val).ok().and_then(|s| s.parse::<i64>().ok()) {
                                Some(num) => num,
                                None => {
                                    error = Some(b"-ERR Value is not an integer or out of range\r\n" as &[u8]);
                                    0
                                }
                            }
                        }
                        _ => {
                            error = Some(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" as &[u8]);
                            0
                        }
                    },
                    None => 0,
                };

                if let Some(err_msg) = error {
                    write_buffer.extend_from_slice(err_msg);
                } else {
                    let new_num = current_number + 1;
                    let new_entry = Entry {
                        value: crate::engine::DataType::String(Bytes::from(new_num.to_string())),
                        expires_at: None,
                    };
                    map.insert(key.clone(), new_entry);

                    let log = format!("INCR {}\n", bytes_to_str(&key));
                    let _ = db.aof_tx.send(log).await;

                    write!(&mut write_buffer, ":{}\r\n", new_num).unwrap();
                }
            }
            Command::Publish(channel, message) => {
                // FLUSH BEFORE HANDOFF
                if !write_buffer.is_empty() {
                    let _ = stream.write_all(&write_buffer).await;
                    write_buffer.clear();
                }
                crate::pubsub::handle_publish(&pubsub, bytes_to_str(&channel), bytes_to_str(&message), &mut stream).await;
            }
            Command::Subscribe(channel) => {
                // FLUSH BEFORE HANDOFF
                if !write_buffer.is_empty() {
                    let _ = stream.write_all(&write_buffer).await;
                    write_buffer.clear();
                }
                crate::pubsub::handle_subscribe(&pubsub, bytes_to_str(&channel), &mut stream, &mut reader).await;
                break;
            }
            Command::Unsubscribe(channel) => {
                let ch = bytes_to_str(&channel);
                write!(&mut write_buffer, "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:0\r\n", channel.len(), ch).unwrap();
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

                        let log = format!("LPUSH {} \"{}\"\n", bytes_to_str(&key), bytes_to_str(&value));
                        let _ = db.aof_tx.send(log).await;

                        write!(&mut write_buffer, ":{}\r\n", len).unwrap();
                    }
                    _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value \r\n"),
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
                                write_bulk!(write_buffer, val);
                            } else {
                                write_buffer.extend_from_slice(b"$-1\r\n");
                            }
                        }
                        _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    }
                } else {
                    write_buffer.extend_from_slice(b"$-1\r\n");
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
                                write_bulk!(write_buffer, val);
                            } else {
                                write_buffer.extend_from_slice(b"$-1\r\n");
                            }
                        }
                        _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    }
                } else {
                    write_buffer.extend_from_slice(b"$-1\r\n");
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

                                while (list.len() as i32) > e + 1 { list.pop_back(); }
                                for _ in 0..s { list.pop_front(); }
                                if list.is_empty() { map.remove(&key); }
                            }

                            let log = format!("LTRIM {} {} {}\n", bytes_to_str(&key), start, stop);
                            let _ = db.aof_tx.send(log).await;
                            write_buffer.extend_from_slice(b"+OK\r\n");
                        }
                        _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    }
                } else {
                    write_buffer.extend_from_slice(b"+OK\r\n");
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
                                write_buffer.extend_from_slice(b"*0\r\n");
                            } else {
                                let s = std::cmp::max(0, s) as usize;
                                let e = std::cmp::min(e, len - 1) as usize;
                                let count = e - s + 1;
                                
                                write!(&mut write_buffer, "*{}\r\n", count).unwrap();
                                for i in s..=e {
                                    if let Some(val) = list.get(i) {
                                        write!(&mut write_buffer, "${}\r\n", val.len()).unwrap();
                                        write_buffer.extend_from_slice(val);
                                        write_buffer.extend_from_slice(b"\r\n");
                                    }
                                }
                            }
                        }
                        _ => write_buffer.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
                    }
                } else {
                    write_buffer.extend_from_slice(b"*0\r\n");
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

                    let log = format!("RPUSH {} \"{}\"\n", bytes_to_str(&key), bytes_to_str(&value));
                    let _ = db.aof_tx.send(log).await;

                    write!(&mut write_buffer, ":{}\r\n", len).unwrap();
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

                    let log = format!("HSET {} {} \"{}\"\n", bytes_to_str(&key), bytes_to_str(&field), bytes_to_str(&value));
                    let _ = db.aof_tx.send(log).await;

                    write_buffer.extend_from_slice(b"+OK\r\n");
                } else {
                    write_buffer.extend_from_slice(b"-WRONGTYPE\r\n");
                }
            }
            Command::HGetAll(key) => {
                let map = db.read_shard(&key).await;
                if let Some(entry) = map.get(&key) {
                    if let crate::engine::DataType::Hash(hmap) = &entry.value {
                        write!(&mut write_buffer, "*{}\r\n", hmap.len() * 2).unwrap();
                        for (f, v) in hmap {
                            write!(&mut write_buffer, "${}\r\n", f.len()).unwrap();
                            write_buffer.extend_from_slice(f);
                            write_buffer.extend_from_slice(b"\r\n");
                            write!(&mut write_buffer, "${}\r\n", v.len()).unwrap();
                            write_buffer.extend_from_slice(v);
                            write_buffer.extend_from_slice(b"\r\n");
                        }
                    } else {
                        write_buffer.extend_from_slice(b"-WRONGTYPE\r\n");
                    }
                } else {
                    write_buffer.extend_from_slice(b"*0\r\n");
                }
            }
            Command::HGet(key, field) => {
                let map = db.read_shard(&key).await;
                if let Some(entry) = map.get(&key) {
                    if let crate::engine::DataType::Hash(hmap) = &entry.value {
                        match hmap.get(&field) {
                            Some(val) => write_bulk!(write_buffer, val),
                            None => write_buffer.extend_from_slice(b"$-1\r\n"),
                        }
                    } else {
                        write_buffer.extend_from_slice(b"-WRONGTYPE\r\n");
                    }
                } else {
                    write_buffer.extend_from_slice(b"$-1\r\n");
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
                        let log = format!("RPOPLPUSH {} {}\n", bytes_to_str(&source), bytes_to_str(&destination));
                        let _ = db.aof_tx.send(log).await;
                        write_bulk!(write_buffer, val);
                    }
                    None => write_buffer.extend_from_slice(b"$-1\r\n"),
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
                    let log = format!("LREM {} 1 \"{}\"\n", bytes_to_str(&key), bytes_to_str(&value_to_remove));
                    let _ = db.aof_tx.send(log).await;
                    write_buffer.extend_from_slice(b":1\r\n");
                } else {
                    write_buffer.extend_from_slice(b":0\r\n");
                }
            }
            Command::MGet(key_a, key_b) => {
                let mut val_a: Option<Bytes> = None;
                let mut val_b: Option<Bytes> = None;

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

                write_buffer.extend_from_slice(b"*2\r\n");
                for val in &[val_a, val_b] {
                    match val {
                        Some(s) => {
                            write!(&mut write_buffer, "${}\r\n", s.len()).unwrap();
                            write_buffer.extend_from_slice(s);
                            write_buffer.extend_from_slice(b"\r\n");
                        }
                        None => write_buffer.extend_from_slice(b"$-1\r\n"),
                    }
                }
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
                        let log = format!("SADD {} {}\n", bytes_to_str(&key), bytes_to_str(&member));
                        let _ = db.aof_tx.send(log).await;
                    }
                }

                write!(&mut write_buffer, ":{}\r\n", added).unwrap();
            }
            Command::SInter(key_a, key_b) => {
                let mut set_a = std::collections::HashSet::<Bytes>::new();
                let mut set_b = std::collections::HashSet::<Bytes>::new();

                match db.read_multi_shards(&key_a, &key_b).await {
                    MultiReadGuard::Single(shard) => {
                        if let Some(e) = shard.get(&key_a) {
                            if let crate::engine::DataType::Set(s) = &e.value { set_a = s.clone(); }
                        }
                        if let Some(e) = shard.get(&key_b) {
                            if let crate::engine::DataType::Set(s) = &e.value { set_b = s.clone(); }
                        }
                    }
                    MultiReadGuard::Double(shard_a, shard_b) => {
                        if let Some(e) = shard_a.get(&key_a) {
                            if let crate::engine::DataType::Set(s) = &e.value { set_a = s.clone(); }
                        }
                        if let Some(e) = shard_b.get(&key_b) {
                            if let crate::engine::DataType::Set(s) = &e.value { set_b = s.clone(); }
                        }
                    }
                }

                let intersection: Vec<&Bytes> = set_a.iter().filter(|item| set_b.contains(*item)).collect();

                write!(&mut write_buffer, "*{}\r\n", intersection.len()).unwrap();
                for item in intersection {
                    write!(&mut write_buffer, "${}\r\n", item.len()).unwrap();
                    write_buffer.extend_from_slice(item);
                    write_buffer.extend_from_slice(b"\r\n");
                }
            }
            Command::Keys(pattern) => {
                let pattern_str = bytes_to_str(&pattern);
                let regex_string = format!("^{}$", pattern_str.replace("*", ".*").replace("?", "."));

                let matcher = match regex::Regex::new(&regex_string) {
                    Ok(re) => re,
                    Err(_) => {
                        write_buffer.extend_from_slice(b"-ERR invalid pattern format\r\n");
                        return; // Or skip, depending on your loop control
                    }
                };

                let all_keys = db.get_all_keys().await;
                let filtered_keys: Vec<Bytes> = all_keys
                    .into_iter()
                    .filter(|key| std::str::from_utf8(key).map(|s| matcher.is_match(s)).unwrap_or(false))
                    .collect();

                write!(&mut write_buffer, "*{}\r\n", filtered_keys.len()).unwrap();
                for key in &filtered_keys {
                    write!(&mut write_buffer, "${}\r\n", key.len()).unwrap();
                    write_buffer.extend_from_slice(key);
                    write_buffer.extend_from_slice(b"\r\n");
                }
            }
            Command::Scan(cursor, match_pattern) => {
                if cursor >= 64 {
                    write_buffer.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
                } else {
                    let mut keys = db.scan_shard(cursor).await;

                    if let Some(pattern) = match_pattern {
                        let pattern_str = bytes_to_str(&pattern);
                        let regex_string = format!("^{}$", pattern_str.replace("*", ".*").replace("?", "."));
                        if let Ok(matcher) = regex::Regex::new(&regex_string) {
                            keys.retain(|key| std::str::from_utf8(key).map(|s| matcher.is_match(s)).unwrap_or(false));
                        }
                    }

                    let next_cursor = if cursor == 63 { 0 } else { cursor + 1 };
                    let next_cursor_str = next_cursor.to_string();

                    write!(&mut write_buffer, "*2\r\n${}\r\n{}\r\n*{}\r\n", next_cursor_str.len(), next_cursor_str, keys.len()).unwrap();
                    for key in &keys {
                        write!(&mut write_buffer, "${}\r\n", key.len()).unwrap();
                        write_buffer.extend_from_slice(key);
                        write_buffer.extend_from_slice(b"\r\n");
                    }
                }
            }
            Command::Monitor => {
                // FLUSH BEFORE ENTERING MONITOR LOOP
                write_buffer.extend_from_slice(b"+OK\r\n");
                if stream.write_all(&write_buffer).await.is_err() {
                    break;
                }
                write_buffer.clear();

                let mut rx = db.tx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            let response = format!("+{}\r\n", msg);
                            if stream.write_all(response.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                break; // Monitor permanently hijacks the connection, so break out when done
            }
            _ => {
                write_buffer.extend_from_slice(b"-ERR unknown command\r\n");
            }
        }
    }
}

pub async fn run(address: &str, db: Db, pubsub: PubSub, config: Config) {
    let listener = TcpListener::bind(address).await.expect("Could not bind to address");
    crate::log_success!("Server", "Titan KV natively deployed and listening on {}", address);
    let shared_password = Arc::new(config.requirepass);

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
                    handle_connection(stream, db_handle, pubsub_handle, socket_addr, clients_handle, requirepass).await;
                });
            }
            Err(e) => {
                crate::log_error!("Server", "Connection Failed: {}", e);
            }
        };
    }
}
