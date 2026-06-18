// src/server.rs
//! Titan KV Networking & Command Dispatch
//! 
//! This module handles TCP connections, RESP protocol parsing, 
//! authentication states, and the high-level command execution loop.

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
use crate::config::Config;
use subtle::ConstantTimeEq;

/// Tracks if a connection has proven its identity via AUTH.
#[derive(PartialEq)]
enum ConnectionState {
    Unauthorized,
    Authenticated,
}

/// Metadata about a connected client for internal tracking.
struct ClientInfo {
    id: u64,
    addr: SocketAddr,
    connected_at: SystemTime,
}

/// Monotonic ID counter for clients.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// RAII Guard that removes a client from the active client map when their connection drops.
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

/// DSL-like macro for writing RESP Bulk Strings ($<len>\r\n<data>\r\n) to the buffer.
macro_rules! write_bulk {
    ($buffer:expr, $val:expr) => {{
        let header = format!("${}\r\n", $val.len());
        $buffer.extend_from_slice(header.as_bytes());
        $buffer.extend_from_slice(&$val[..]);
        $buffer.extend_from_slice(b"\r\n");
    }};
}

/// Helper for safe UTF-8 conversion in logs.
fn bytes_to_str(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

/// The core connection handler. One task is spawned per TCP connection.
async fn handle_connection(
    stream: TcpStream,
    db: Db,
    pubsub: PubSub,
    socket_addr: SocketAddr,
    active_clients: Arc<Mutex<HashMap<SocketAddr, ClientInfo>>>,
    requirepass: Arc<Option<String>>
) {
    // Ensure the client is unregistered when this future resolves (disconnect).
    let _guard = ClientGuard {
        addr: socket_addr,
        clients: active_clients.clone(),
    };

    // Split the stream into read/write halves for concurrent I/O (required for PubSub).
    let (read_half, mut stream) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Initialize security state based on server config.
    let mut state = if requirepass.is_some() {
        ConnectionState::Unauthorized
    } else {
        ConnectionState::Authenticated
    };

    // Reusable buffer to minimize allocations during response writing.
    let mut write_buffer = Vec::with_capacity(8192);

    loop {
        // 1. RECV & PARSE RESP PACKET
        let parts: Vec<Bytes> = match read_resp(&mut reader).await {
            Ok(p) if !p.is_empty() => p,
            _ => {
                crate::log_info!("Client", "Client Disconnected.");
                break;
            }
        };

        let command = parse_command(&parts);

        // 2. LOGGING & MONITORING
        // We log commands before execution for auditing, even if they fail auth.
        let summary_parts: Vec<String> = parts
            .iter()
            .map(|p| {
                let s = bytes_to_str(p);
                if s.len() > 30 { format!("{}...({}b)", &s[..15], s.len()) } else { s.to_string() }
            })
            .collect();
        crate::log_info!("Command", "{}", summary_parts.join(" "));

        // Broadcast to all clients currently running the MONITOR command.
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

        // 3. SECURITY GATEWAY
        if state == ConnectionState::Unauthorized {
            match command {
                Command::Auth(provided_password) => {
                    let actual_password = requirepass.as_deref().unwrap();
                    // We use subtle::ConstantTimeEq to prevent timing attacks where 
                    // an attacker guesses the password char-by-char.
                    if provided_password.as_bytes().ct_eq(actual_password.as_bytes()).into() {
                        state = ConnectionState::Authenticated;
                        write_buffer.extend_from_slice(b"+OK\r\n");
                    } else {
                        write_buffer.extend_from_slice(b"-ERR invalid password\r\n");
                    }
                }
                _ => {
                    write_buffer.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
                }
            }

            let _ = stream.write_all(&write_buffer).await;
            write_buffer.clear();
            continue; // Skip the database execution logic
        }

        // ============================================================
// COMMAND DISPATCH HANDLER
//
// This is the core match block that routes each parsed Redis
// command to its corresponding handler logic. Each arm:
//   1. Reads or writes to the appropriate shard of the database
//   2. Appends a response in RESP (Redis Serialization Protocol)
//      format to `write_buffer`
//   3. Optionally ships a log entry to the AOF (Append-Only File)
//      worker via an async channel for durability
//
// RESP format recap:
//   +OK\r\n          → Simple string
//   -ERR msg\r\n     → Error
//   :42\r\n          → Integer
//   $6\r\nfoobar\r\n → Bulk string (length-prefixed)
//   *3\r\n...        → Array of N elements
//   $-1\r\n          → Null bulk string (key not found)
// ============================================================
match command {

    // ----------------------------------------------------------
    // AUTH
    // Sent by clients that connect with a password requirement.
    // This implementation accepts any auth token and always
    // responds with +OK. For a real password check, you'd
    // validate the token inside Command::Auth(token) here.
    // ----------------------------------------------------------
    Command::Auth(_) => {
        write_buffer.extend_from_slice(b"+OK\r\n");
    }

    // ----------------------------------------------------------
    // CLIENT LIST
    // Returns metadata about all currently connected clients.
    // This mimics Redis's CLIENT LIST output format, which is a
    // newline-separated list of key=value pairs per client.
    //
    // Process:
    //   1. Lock the shared `active_clients` map (briefly, to copy)
    //   2. For each client, compute how long they've been connected
    //      using SystemTime arithmetic
    //   3. Build the full string, then send it as a RESP bulk string
    //      using the $<length>\r\n<data>\r\n format
    //
    // Note: fd=-1 is a placeholder — we don't expose real file
    // descriptor numbers from Tokio's async runtime.
    // ----------------------------------------------------------
    Command::ClientList => {
        let mut buf = String::new();
        let now = SystemTime::now();
        {
            // Acquire the mutex lock. This scope ensures the lock
            // is released as soon as we're done iterating.
            let clients = active_clients.lock().unwrap();
            for info in clients.values() {
                // Calculate how many seconds this client has been connected.
                // duration_since returns Err if `now` is earlier (clock skew),
                // so we default to 0 seconds in that case.
                let age = now
                    .duration_since(info.connected_at)
                    .unwrap_or_default()
                    .as_secs();

                // Format a Redis-compatible client info line. Each field is
                // space-separated as key=value. This matches what redis-cli
                // and monitoring tools expect to parse.
                buf.push_str(&format!(
                    "id={} addr={} fd=-1 name= age={} idle=0 flags=N db=0 \
                     sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 \
                     omem=0 events=r cmd=client\n",
                    info.id, info.addr, age
                ));
            }
        }
        // Encode as a RESP bulk string: $<byte_length>\r\n<content>\r\n
        write!(&mut write_buffer, "${}\r\n{}\r\n", buf.len(), buf).unwrap();
    }

    // ----------------------------------------------------------
    // STRLEN <key>
    // Returns the byte-length of the string value stored at key.
    //
    // RESP return values:
    //   :<n>\r\n   → length of the string
    //   :0\r\n     → key does not exist (Redis treats missing as empty)
    //   -WRONGTYPE → key exists but holds a non-string type
    // ----------------------------------------------------------
    Command::Strlen(key) => {
        // We only need a read lock here since we're not modifying anything.
        let map = db.read_shard(&key).await;
        match map.get(&key) {
            Some(entry) => match &entry.value {
                crate::engine::DataType::String(val) => {
                    // val is a Bytes object; .len() gives the byte count.
                    write!(&mut write_buffer, ":{}\r\n", val.len()).unwrap();
                }
                // Any non-string type returns a WRONGTYPE error, consistent
                // with Redis's type-safety model.
                _ => write_buffer.extend_from_slice(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                ),
            },
            // Key doesn't exist → length is 0 per Redis spec.
            None => write_buffer.extend_from_slice(b":0\r\n"),
        };
    }

    // ----------------------------------------------------------
    // DBSIZE
    // Returns the total number of keys across ALL shards.
    //
    // Because this engine uses sharded hashmaps to reduce lock
    // contention, we must iterate every shard and sum their lengths.
    // This is O(num_shards), not O(total_keys), since HashMap::len()
    // is O(1) per shard.
    // ----------------------------------------------------------
    Command::DbSize => {
        let mut total_keys = 0;
        // Iterate over every shard index. get_shard_count() tells us
        // how many shards were configured at startup.
        for i in 0..db.get_shard_count() {
            let map = db.read_shard_by_index(i).await;
            total_keys += map.len();
        }
        write!(&mut write_buffer, ":{}\r\n", total_keys).unwrap();
    }

    // ----------------------------------------------------------
    // TYPE <key>
    // Returns the Redis type name of the value stored at key.
    // Redis clients use this to decide how to interact with a key
    // (e.g., whether to use list commands or hash commands).
    //
    // RESP returns a simple string (+<type>\r\n):
    //   +string, +list, +hash, +set, or +none (key not found)
    // ----------------------------------------------------------
    Command::Type(key) => {
        let map = db.read_shard(&key).await;
        match map.get(&key) {
            Some(entry) => {
                // Match on the internal DataType enum and map it to
                // the Redis protocol string representation.
                let type_str = match &entry.value {
                    crate::engine::DataType::String(_) => "string",
                    crate::engine::DataType::List(_)   => "list",
                    crate::engine::DataType::Hash(_)   => "hash",
                    crate::engine::DataType::Set(_)    => "set",
                };
                write!(&mut write_buffer, "+{}\r\n", type_str).unwrap();
            }
            // Redis returns "+none" (not nil/null) when the key doesn't exist.
            None => write_buffer.extend_from_slice(b"+none\r\n"),
        }
    }

    // ----------------------------------------------------------
    // TTL <key>
    // Returns the remaining time-to-live for a key in SECONDS.
    //
    // RESP integer responses:
    //   :<n>\r\n  → seconds remaining (positive)
    //   :-1\r\n   → key exists but has no expiry (persistent)
    //   :-2\r\n   → key does not exist, OR has already expired
    //
    // Note: We do NOT delete the expired key here. Lazy deletion
    // happens during GET. TTL just reports the state.
    // ----------------------------------------------------------
    Command::Ttl(key) => {
        let map = db.read_shard(&key).await;
        match map.get(&key) {
            Some(entry) => match entry.expires_at {
                Some(expiration) => {
                    let now = SystemTime::now();
                    if now >= expiration {
                        // Key has already expired. Return -2 to signal
                        // "key does not exist" from the client's perspective.
                        write_buffer.extend_from_slice(b":-2\r\n");
                    } else {
                        // Compute remaining duration. duration_since on a
                        // future time requires flipping the operands.
                        let duration = expiration.duration_since(now).unwrap_or_default();
                        write!(&mut write_buffer, ":{}\r\n", duration.as_secs()).unwrap();
                    }
                }
                // No expiration set → key lives forever → return -1.
                None => write_buffer.extend_from_slice(b":-1\r\n"),
            },
            // Key doesn't exist at all → return -2.
            None => write_buffer.extend_from_slice(b":-2\r\n"),
        }
    }

    // ----------------------------------------------------------
    // PTTL <key>
    // Identical to TTL but returns remaining time in MILLISECONDS.
    // This gives clients higher precision for short-lived keys.
    //
    // Uses .as_millis() instead of .as_secs() — everything else
    // is structurally identical to the TTL handler above.
    // ----------------------------------------------------------
    Command::Pttl(key) => {
        let map = db.read_shard(&key).await;
        match map.get(&key) {
            Some(entry) => match entry.expires_at {
                Some(expiration) => {
                    let now = SystemTime::now();
                    if now >= expiration {
                        write_buffer.extend_from_slice(b":-2\r\n");
                    } else {
                        let duration = expiration.duration_since(now).unwrap_or_default();
                        // as_millis() returns u128; Redis clients expect this
                        // to fit in a signed 64-bit integer, which is safe
                        // for any reasonable TTL value.
                        write!(&mut write_buffer, ":{}\r\n", duration.as_millis()).unwrap();
                    }
                }
                None => write_buffer.extend_from_slice(b":-1\r\n"),
            },
            None => write_buffer.extend_from_slice(b":-2\r\n"),
        }
    }

    // ----------------------------------------------------------
    // SETEX <key> <seconds> <value>
    // Sets a key to a string value with an explicit TTL in seconds.
    // This is equivalent to SET key value EX seconds in modern Redis.
    //
    // Steps:
    //   1. Compute the absolute expiration time (now + duration)
    //   2. Build an Entry with expires_at set
    //   3. Write to the appropriate shard
    //   4. Log to AOF for persistence
    // ----------------------------------------------------------
    Command::SetEx(key, seconds, value) => {
        // Compute the wall-clock time at which this key should expire.
        // SystemTime::now() + Duration gives us an absolute SystemTime.
        let expiration_time = SystemTime::now() + Duration::from_secs(seconds as u64);

        let new_entry = Entry {
            value: crate::engine::DataType::String(value.clone()),
            expires_at: Some(expiration_time),
        };

        // Acquire a write lock on the shard that owns this key.
        let mut map = db.write_shard(&key).await;
        map.insert(key.clone(), new_entry);

        // AOF durability: send the command string to the background
        // AOF writer. The channel send is fire-and-forget (we ignore
        // the error) to avoid blocking the client response on disk I/O.
        let log = format!(
            "SETEX {} {} {}\n",
            bytes_to_str(&key),
            seconds,
            bytes_to_str(&value)
        );
        let _ = db.aof_tx.send(log).await;

        write_buffer.extend_from_slice(b"+OK\r\n");
    }

    // ----------------------------------------------------------
    // PING
    // The simplest health-check command. If the server responds
    // with +PONG, the client knows the connection is alive and
    // the server is processing commands.
    // ----------------------------------------------------------
    Command::Ping => {
        write_buffer.extend_from_slice(b"+PONG\r\n");
    }

    // ----------------------------------------------------------
    // SET <key> <value>
    // Stores a string value at key with no expiration.
    // If the key already exists (regardless of its type), it is
    // overwritten — Redis's SET always replaces.
    //
    // Note: This implementation uses a write lock even though
    // HashMap::insert handles both insert and update paths.
    // ----------------------------------------------------------
    Command::Set(key, value) => {
        let mut shard = db.write_shard(&key).await;
        shard.insert(
            key.clone(),
            Entry {
                value: engine::DataType::String(value.clone()),
                expires_at: None, // No TTL → key persists until explicitly deleted
            },
        );

        // Log to AOF. Even for simple SETs, we need to record this
        // so that on restart the key can be replayed into memory.
        let log = format!("SET {} {}\n", bytes_to_str(&key), bytes_to_str(&value));
        let _ = db.aof_tx.send(log).await;

        write_buffer.extend_from_slice(b"+OK\r\n");
    }

    // ----------------------------------------------------------
    // GET <key>
    // Retrieves the string value stored at key.
    //
    // This is where LAZY EXPIRATION is implemented:
    // Rather than running a background sweeper to delete expired
    // keys, we check expiry on every read. If the key is expired:
    //   1. We delete it from the shard
    //   2. Log the DEL to AOF
    //   3. Return $-1 (null bulk string) as if the key never existed
    //
    // This approach avoids a dedicated expiry thread but means
    // expired keys linger in memory until they're accessed.
    //
    // Requires a WRITE lock because we may need to delete.
    // ----------------------------------------------------------
    Command::Get(key) => {
        // We need a write lock here even for reads because lazy
        // deletion may require removing the key from the map.
        let mut map = db.write_shard(&key).await;
        let mut expired = false;

        // First pass: check if the key exists and if it has expired.
        // We don't modify the map yet because we're still holding
        // an immutable borrow via map.get().
        if let Some(entry) = map.get(&key) {
            if let Some(expiration) = entry.expires_at {
                if SystemTime::now() > expiration {
                    expired = true;
                }
            }
        }

        if expired {
            // Remove the expired key. map.remove returns Some(entry)
            // if the key existed, which it should since we just found it.
            if map.remove(&key).is_some() {
                // Log the deletion to AOF so a server restart doesn't
                // resurrect the expired key from the log.
                let log = format!("DEL {}\n", bytes_to_str(&key));
                let _ = db.aof_tx.send(log).await;
            }
            // Return null bulk string — same response as "key not found".
            write_buffer.extend_from_slice(b"$-1\r\n");
        } else {
            match map.get(&key) {
                Some(entry) => match &entry.value {
                    crate::engine::DataType::String(val) => {
                        // write_bulk! is a macro that emits:
                        //   $<len>\r\n<bytes>\r\n
                        write_bulk!(write_buffer, val);
                    }
                    // The key exists but holds a list, hash, or set.
                    // Return WRONGTYPE error per Redis spec.
                    _ => write_buffer.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong type of value\r\n",
                    ),
                },
                // Key genuinely doesn't exist.
                None => write_buffer.extend_from_slice(b"$-1\r\n"),
            }
        }
    }

    // ----------------------------------------------------------
    // DEL <key>
    // Removes a key from the database.
    //
    // RESP integer response:
    //   :1\r\n → key existed and was deleted
    //   :0\r\n → key did not exist (no-op)
    // ----------------------------------------------------------
    Command::Del(key) => {
        let mut map = db.write_shard(&key).await;

        // HashMap::remove returns Some(value) if the key existed,
        // None otherwise. We use this to decide the integer response.
        if map.remove(&key).is_some() {
            // Log the deletion to AOF for durability.
            let log = format!("DEL {}\n", bytes_to_str(&key));
            let _ = db.aof_tx.send(log).await;
            write_buffer.extend_from_slice(b":1\r\n");
        } else {
            write_buffer.extend_from_slice(b":0\r\n");
        }
    }

    // ----------------------------------------------------------
    // EXISTS <key>
    // Checks whether a key is present in the database.
    // Does NOT check expiry — a key that has expired but hasn't
    // been lazily deleted yet would return :1 here. In a production
    // system you'd want to add an expiry check here too.
    //
    // RESP: :1 if exists, :0 if not
    // ----------------------------------------------------------
    Command::Exists(key) => {
        // Read lock is sufficient — no mutations needed.
        let map = db.read_shard(&key).await;
        if map.contains_key(&key) {
            write_buffer.extend_from_slice(b":1\r\n");
        } else {
            write_buffer.extend_from_slice(b":0\r\n");
        }
    }

    // ----------------------------------------------------------
    // INCR <key>
    // Atomically increments the integer value of a string key by 1.
    // If the key doesn't exist, it's initialized to 0 before incrementing
    // (resulting in a stored value of 1).
    //
    // Error conditions:
    //   - Key holds a non-string type → WRONGTYPE
    //   - Key's string value cannot be parsed as i64 → ERR
    //
    // Implementation note: We use a two-phase approach —
    // read the current value first (with error handling), then
    // only write the new value if parsing succeeded.
    // ----------------------------------------------------------
    Command::Incr(key) => {
        let mut map = db.write_shard(&key).await;

        // We use an Option<&[u8]> to carry error messages so we
        // can avoid borrowing issues when we later need to mutate `map`.
        let mut error = None;

        let current_number = match map.get(&key) {
            Some(entry) => match &entry.value {
                crate::engine::DataType::String(val) => {
                    // Attempt to decode bytes as UTF-8, then parse as i64.
                    // Both steps can fail, in which case we set the error flag.
                    match std::str::from_utf8(val)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(num) => num,
                        None => {
                            error = Some(
                                b"-ERR Value is not an integer or out of range\r\n" as &[u8],
                            );
                            0 // Placeholder; not used if error is set
                        }
                    }
                }
                _ => {
                    error = Some(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                            as &[u8],
                    );
                    0
                }
            },
            // Key doesn't exist: Redis treats this as 0 before incrementing.
            None => 0,
        };

        if let Some(err_msg) = error {
            // Return the error and skip the write entirely.
            write_buffer.extend_from_slice(err_msg);
        } else {
            let new_num = current_number + 1;

            // Store the new value back as a string (Redis always stores
            // integers as their decimal string representation).
            let new_entry = Entry {
                value: crate::engine::DataType::String(Bytes::from(new_num.to_string())),
                expires_at: None, // INCR does not reset or set expiry
            };
            map.insert(key.clone(), new_entry);

            // Log to AOF. We log INCR (not SET) so AOF replay is idempotent
            // and semantically equivalent.
            let log = format!("INCR {}\n", bytes_to_str(&key));
            let _ = db.aof_tx.send(log).await;

            // Return the new integer value.
            write!(&mut write_buffer, ":{}\r\n", new_num).unwrap();
        }
    }

    // ----------------------------------------------------------
    // LPUSH <key> <value>
    // Prepends a value to the HEAD of a list.
    // Creates the list if it doesn't exist.
    //
    // Returns the length of the list after the push.
    //
    // Internal storage: VecDeque, which supports O(1) push_front
    // and push_back, making it ideal for Redis list semantics.
    // ----------------------------------------------------------
    Command::LPush(key, value) => {
        let mut map = db.write_shard(&key).await;

        // `entry().or_insert_with()` atomically creates the key with
        // an empty list if it doesn't already exist. This avoids a
        // separate exists-check + insert pattern.
        let entry = map.entry(key.clone()).or_insert_with(|| Entry {
            value: crate::engine::DataType::List(std::collections::VecDeque::new()),
            expires_at: None,
        });

        match &mut entry.value {
            crate::engine::DataType::List(list) => {
                // push_front → prepend to head (LPUSH semantics).
                list.push_front(value.clone());
                let len = list.len();

                let log = format!(
                    "LPUSH {} \"{}\"\n",
                    bytes_to_str(&key),
                    bytes_to_str(&value)
                );
                let _ = db.aof_tx.send(log).await;

                // Return new list length as a RESP integer.
                write!(&mut write_buffer, ":{}\r\n", len).unwrap();
            }
            // Key existed but is not a list.
            _ => write_buffer.extend_from_slice(
                b"-WRONGTYPE Operation against a key holding the wrong kind of value \r\n",
            ),
        }
    }

    // ----------------------------------------------------------
    // LPOP <key>
    // Removes and returns the element at the HEAD (left side) of a list.
    //
    // Returns:
    //   Bulk string of the popped value
    //   $-1 if the list is empty or the key doesn't exist
    // ----------------------------------------------------------
    Command::LPop(key) => {
        let mut map = db.write_shard(&key).await;
        if let Some(entry) = map.get_mut(&key) {
            match &mut entry.value {
                crate::engine::DataType::List(list) => {
                    if let Some(val) = list.pop_front() {
                        // Log the pop. Note: we can't log the value itself
                        // here because LPOP is position-based, not value-based.
                        let log = format!("LPOP {}\n", bytes_to_str(&key));
                        let _ = db.aof_tx.send(log).await;
                        write_bulk!(write_buffer, val);
                    } else {
                        // List exists but is empty.
                        write_buffer.extend_from_slice(b"$-1\r\n");
                    }
                }
                _ => write_buffer.extend_from_slice(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                ),
            }
        } else {
            // Key doesn't exist.
            write_buffer.extend_from_slice(b"$-1\r\n");
        }
    }

    // ----------------------------------------------------------
    // RPUSH <key> <value>
    // Appends a value to the TAIL (right side) of a list.
    // Creates the list if it doesn't exist.
    // Mirror of LPUSH but uses push_back instead of push_front.
    // ----------------------------------------------------------
    Command::RPush(key, value) => {
        let mut map = db.write_shard(&key).await;
        let entry = map.entry(key.clone()).or_insert_with(|| Entry {
            value: crate::engine::DataType::List(std::collections::VecDeque::new()),
            expires_at: None,
        });

        if let crate::engine::DataType::List(list) = &mut entry.value {
            // push_back → append to tail (RPUSH semantics).
            list.push_back(value.clone());
            let len = list.len();

            let log = format!(
                "RPUSH {} \"{}\"\n",
                bytes_to_str(&key),
                bytes_to_str(&value)
            );
            let _ = db.aof_tx.send(log).await;

            write!(&mut write_buffer, ":{}\r\n", len).unwrap();
        }
        // Note: if the key exists but is not a list, this silently
        // does nothing. A production implementation should return WRONGTYPE here.
    }

    // ----------------------------------------------------------
    // RPOP <key>
    // Removes and returns the element at the TAIL (right side) of a list.
    // Mirror of LPOP but uses pop_back instead of pop_front.
    // ----------------------------------------------------------
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
                _ => write_buffer.extend_from_slice(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                ),
            }
        } else {
            write_buffer.extend_from_slice(b"$-1\r\n");
        }
    }

    // ----------------------------------------------------------
    // LTRIM <key> <start> <stop>
    // Trims a list to only contain elements within [start, stop].
    // Both indices are 0-based and support negative indexing:
    //   -1 → last element, -2 → second to last, etc.
    //
    // After trimming, elements outside the range are discarded.
    // If the range is invalid (start > end, or out of bounds entirely),
    // the entire list is deleted.
    //
    // Algorithm:
    //   1. Normalize negative indices to positive ones
    //   2. Remove elements from the back until we reach `stop`
    //   3. Remove elements from the front until we reach `start`
    // ----------------------------------------------------------
    Command::LTrim(key, start, stop) => {
        let mut map = db.write_shard(&key).await;
        if let Some(entry) = map.get_mut(&key) {
            match &mut entry.value {
                crate::engine::DataType::List(list) => {
                    let len = list.len() as i32;

                    // Resolve negative indices into positive absolute positions.
                    // e.g., if len=5 and start=-2, s = 5 + (-2) = 3
                    let mut s = if start < 0 { len + start } else { start };
                    let mut e = if stop < 0 { len + stop } else { stop };

                    if s > e || s >= len {
                        // The range is entirely invalid or out of bounds.
                        // Redis deletes the key in this case.
                        map.remove(&key);
                    } else {
                        // Clamp to valid bounds.
                        s = std::cmp::max(0, s);
                        e = std::cmp::min(e, len - 1);

                        // Trim the tail: pop elements until the list ends at index `e`.
                        while (list.len() as i32) > e + 1 {
                            list.pop_back();
                        }
                        // Trim the head: pop `s` elements from the front.
                        for _ in 0..s {
                            list.pop_front();
                        }

                        // If trimming resulted in an empty list, remove the key.
                        // Redis does not store empty lists.
                        if list.is_empty() {
                            map.remove(&key);
                        }
                    }

                    let log = format!(
                        "LTRIM {} {} {}\n",
                        bytes_to_str(&key),
                        start,
                        stop
                    );
                    let _ = db.aof_tx.send(log).await;
                    write_buffer.extend_from_slice(b"+OK\r\n");
                }
                _ => write_buffer.extend_from_slice(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                ),
            }
        } else {
            // Key doesn't exist — LTRIM on a missing key is a no-op in Redis.
            write_buffer.extend_from_slice(b"+OK\r\n");
        }
    }

    // ----------------------------------------------------------
    // LRANGE <key> <start> <stop>
    // Returns a slice of elements from a list, inclusive on both ends.
    // Supports negative indices (same semantics as LTRIM).
    //
    // Returns a RESP array of bulk strings.
    // Returns *0\r\n if range is empty or key doesn't exist.
    //
    // This is a READ operation — no write lock needed.
    // ----------------------------------------------------------
    Command::LRange(key, start, stop) => {
        let map = db.read_shard(&key).await;
        if let Some(entry) = map.get(&key) {
            match &entry.value {
                crate::engine::DataType::List(list) => {
                    let len = list.len() as i32;

                    // Resolve negative indices.
                    let s = if start < 0 { len + start } else { start };
                    let e = if stop < 0 { len + stop } else { stop };

                    // Validate the range. Any of these conditions means
                    // there are no elements to return.
                    if s > e || s >= len || e < 0 {
                        write_buffer.extend_from_slice(b"*0\r\n");
                    } else {
                        // Clamp both ends to valid index range.
                        let s = std::cmp::max(0, s) as usize;
                        let e = std::cmp::min(e, len - 1) as usize;
                        let count = e - s + 1;

                        // Write the RESP array header: *<count>\r\n
                        write!(&mut write_buffer, "*{}\r\n", count).unwrap();

                        // Write each element as a bulk string.
                        // VecDeque::get(i) provides O(1) random access.
                        for i in s..=e {
                            if let Some(val) = list.get(i) {
                                write!(&mut write_buffer, "${}\r\n", val.len()).unwrap();
                                write_buffer.extend_from_slice(val);
                                write_buffer.extend_from_slice(b"\r\n");
                            }
                        }
                    }
                }
                _ => write_buffer.extend_from_slice(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                ),
            }
        } else {
            // Missing key → empty array.
            write_buffer.extend_from_slice(b"*0\r\n");
        }
    }

    // ----------------------------------------------------------
    // HSET <key> <field> <value>
    // Sets a single field in a hash map stored at key.
    // Creates the hash if it doesn't exist.
    //
    // In Redis 4+, HSET can set multiple fields at once. This
    // implementation handles one field at a time.
    // ----------------------------------------------------------
    Command::HSet(key, field, value) => {
        let mut map = db.write_shard(&key).await;

        // Create the hash entry if it doesn't exist, or get a mutable
        // reference to the existing one.
        let entry = map.entry(key.clone()).or_insert_with(|| Entry {
            value: crate::engine::DataType::Hash(std::collections::HashMap::new()),
            expires_at: None,
        });

        if let crate::engine::DataType::Hash(hmap) = &mut entry.value {
            // Overwrite the field if it already exists, or insert if new.
            // HashMap::insert handles both cases silently.
            hmap.insert(field.clone(), value.clone());

            let log = format!(
                "HSET {} {} \"{}\"\n",
                bytes_to_str(&key),
                bytes_to_str(&field),
                bytes_to_str(&value)
            );
            let _ = db.aof_tx.send(log).await;
            write_buffer.extend_from_slice(b"+OK\r\n");
        } else {
            // Key exists but is not a hash type.
            write_buffer.extend_from_slice(b"-WRONGTYPE\r\n");
        }
    }

    // ----------------------------------------------------------
    // HGETALL <key>
    // Returns ALL fields and values from a hash, interleaved:
    //   field1, value1, field2, value2, ...
    //
    // RESP: array with 2 * (number of fields) elements.
    // Note: HashMap iteration order is non-deterministic in Rust,
    // so field order may vary between calls. This matches Redis's
    // own behavior for HGETALL.
    // ----------------------------------------------------------
    Command::HGetAll(key) => {
        let map = db.read_shard(&key).await;
        if let Some(entry) = map.get(&key) {
            if let crate::engine::DataType::Hash(hmap) = &entry.value {
                // Each field-value pair contributes 2 elements to the array.
                write!(&mut write_buffer, "*{}\r\n", hmap.len() * 2).unwrap();
                for (f, v) in hmap {
                    // Write field name as bulk string.
                    write!(&mut write_buffer, "${}\r\n", f.len()).unwrap();
                    write_buffer.extend_from_slice(f);
                    write_buffer.extend_from_slice(b"\r\n");
                    // Write field value as bulk string.
                    write!(&mut write_buffer, "${}\r\n", v.len()).unwrap();
                    write_buffer.extend_from_slice(v);
                    write_buffer.extend_from_slice(b"\r\n");
                }
            } else {
                write_buffer.extend_from_slice(b"-WRONGTYPE\r\n");
            }
        } else {
            // Missing key → empty array (not null).
            write_buffer.extend_from_slice(b"*0\r\n");
        }
    }

    // ----------------------------------------------------------
    // HGET <key> <field>
    // Returns the value of a specific field within a hash.
    //
    // Returns:
    //   Bulk string of the field value if found
    //   $-1 if the key doesn't exist or the field isn't in the hash
    // ----------------------------------------------------------
    Command::HGet(key, field) => {
        let map = db.read_shard(&key).await;
        if let Some(entry) = map.get(&key) {
            if let crate::engine::DataType::Hash(hmap) = &entry.value {
                match hmap.get(&field) {
                    Some(val) => write_bulk!(write_buffer, val),
                    // Field doesn't exist within the hash → null bulk string.
                    None => write_buffer.extend_from_slice(b"$-1\r\n"),
                }
            } else {
                write_buffer.extend_from_slice(b"-WRONGTYPE\r\n");
            }
        } else {
            write_buffer.extend_from_slice(b"$-1\r\n");
        }
    }

    // ----------------------------------------------------------
    // RPOPLPUSH <source> <destination>
    // Atomically pops the TAIL element of `source` and pushes it
    // to the HEAD of `destination`. Returns the moved element.
    //
    // This is commonly used to implement reliable queues:
    //   - Workers pop from a "processing" list
    //   - On crash/failure, the item remains in "processing"
    //   - A recovery process can re-queue it
    //
    // The atomicity challenge: source and destination may live in
    // DIFFERENT shards (different mutexes). To handle both cases
    // without deadlock, we use `write_multi_shards()` which returns
    // either a SingleGuard (same shard) or a DoubleGuard (two shards,
    // always locked in a consistent global order to prevent deadlock).
    // ----------------------------------------------------------
    Command::RPopLPush(source, destination) => {
        let mut popped_val = None;

        match db.write_multi_shards(&source, &destination).await {
            // Both keys live in the same shard — single lock acquired.
            MultiWriteGuard::Single(mut shard) => {
                // Pop from source list's tail.
                if let Some(entry) = shard.get_mut(&source) {
                    if let engine::DataType::List(list) = &mut entry.value {
                        popped_val = list.pop_back();
                    }
                }
                // If we got a value, push it to the destination list's head.
                if let Some(val) = &popped_val {
                    let dest_entry =
                        shard.entry(destination.clone()).or_insert_with(|| Entry {
                            value: engine::DataType::List(
                                std::collections::VecDeque::new(),
                            ),
                            expires_at: None,
                        });
                    if let engine::DataType::List(dest_list) = &mut dest_entry.value {
                        dest_list.push_front(val.clone());
                    }
                }
            }
            // Keys live in different shards — two separate locks acquired
            // in a deterministic order to prevent deadlock.
            MultiWriteGuard::Double(mut shard_src, mut shard_dest) => {
                if let Some(entry) = shard_src.get_mut(&source) {
                    if let engine::DataType::List(list) = &mut entry.value {
                        popped_val = list.pop_back();
                    }
                }
                if let Some(val) = &popped_val {
                    let dest_entry =
                        shard_dest.entry(destination.clone()).or_insert_with(|| Entry {
                            value: engine::DataType::List(
                                std::collections::VecDeque::new(),
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
                let log = format!(
                    "RPOPLPUSH {} {}\n",
                    bytes_to_str(&source),
                    bytes_to_str(&destination)
                );
                let _ = db.aof_tx.send(log).await;
                write_bulk!(write_buffer, val);
            }
            // Source list was empty or didn't exist.
            None => write_buffer.extend_from_slice(b"$-1\r\n"),
        }
    }

    // ----------------------------------------------------------
    // LREM <key> <count> <value>
    // Removes occurrences of `value` from a list.
    //
    // Current limitation: this implementation ignores `count` and
    // always removes the FIRST occurrence only (equivalent to count=1).
    // A full Redis-compatible LREM would:
    //   count > 0 → remove `count` occurrences from head
    //   count < 0 → remove `|count|` occurrences from tail
    //   count = 0 → remove all occurrences
    //
    // Returns the number of elements removed (:1 or :0).
    // ----------------------------------------------------------
    Command::LRem(key, _count, value_to_remove) => {
        let mut map = db.write_shard(&key).await;
        let mut removed = false;

        if let Some(entry) = map.get_mut(&key) {
            if let crate::engine::DataType::List(list) = &mut entry.value {
                // Find the first index where the value matches.
                // VecDeque doesn't have a built-in remove-by-value,
                // so we locate the index first, then remove it.
                if let Some(index) = list.iter().position(|x| x == &value_to_remove) {
                    list.remove(index); // O(n) shift but acceptable for small lists
                    removed = true;
                }
            }
        }

        if removed {
            let log = format!(
                "LREM {} 1 \"{}\"\n",
                bytes_to_str(&key),
                bytes_to_str(&value_to_remove)
            );
            let _ = db.aof_tx.send(log).await;
            write_buffer.extend_from_slice(b":1\r\n");
        } else {
            write_buffer.extend_from_slice(b":0\r\n");
        }
    }

    // ----------------------------------------------------------
    // MGET <key_a> <key_b>
    // Returns the values of exactly two keys in a single response.
    // Missing keys return null bulk strings in their position.
    //
    // Current limitation: this supports exactly 2 keys. A full
    // MGET implementation accepts a variadic list of keys.
    //
    // Same cross-shard concern as RPOPLPUSH: the two keys may
    // hash to different shards. We use read_multi_shards() here,
    // which handles both the single-shard and cross-shard case
    // via ReadGuard variants.
    // ----------------------------------------------------------
    Command::MGet(key_a, key_b) => {
        let mut val_a: Option<Bytes> = None;
        let mut val_b: Option<Bytes> = None;

        match db.read_multi_shards(&key_a, &key_b).await {
            MultiReadGuard::Single(shard) => {
                // Both keys in the same shard — one lock covers both reads.
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
                // Different shards — each has its own lock.
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

        // Always return a 2-element RESP array, with $-1 for missing keys.
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

    // ----------------------------------------------------------
    // SADD <key> <member>
    // Adds a member to a set stored at key. Creates the set if needed.
    //
    // Sets in Redis are unordered collections of unique byte strings.
    // Internally we use HashSet<Bytes> for O(1) insert and membership.
    //
    // Returns:
    //   :1 if the member was newly added
    //   :0 if the member already existed (no duplicate added)
    // ----------------------------------------------------------
    Command::SAdd(key, member) => {
        let mut shard = db.write_shard(&key).await;
        let mut added = 0;

        let entry = shard.entry(key.clone()).or_insert_with(|| crate::engine::Entry {
            value: crate::engine::DataType::Set(std::collections::HashSet::new()),
            expires_at: None,
        });

        if let crate::engine::DataType::Set(set) = &mut entry.value {
            // HashSet::insert returns true if the value was NOT already present.
            if set.insert(member.clone()) {
                added = 1;
                let log = format!(
                    "SADD {} {}\n",
                    bytes_to_str(&key),
                    bytes_to_str(&member)
                );
                let _ = db.aof_tx.send(log).await;
            }
            // If insert returns false, the member was already in the set.
            // We do nothing and return :0.
        }
        write!(&mut write_buffer, ":{}\r\n", added).unwrap();
    }

    // ----------------------------------------------------------
    // SMEMBERS <key>
    // Returns all members of a set as a RESP array.
    //
    // Order of elements is not guaranteed — HashSet iteration
    // is non-deterministic. This matches Redis's own behavior.
    // Returns *0\r\n for missing keys (empty set).
    // ----------------------------------------------------------
    Command::SMembers(key) => {
        let shard = db.read_shard(&key).await;
        match shard.get(&key) {
            Some(entry) => match &entry.value {
                crate::engine::DataType::Set(set) => {
                    write!(&mut write_buffer, "*{}\r\n", set.len()).unwrap();
                    for member in set {
                        // Each member is emitted as a bulk string.
                        write_bulk!(write_buffer, member);
                    }
                }
                _ => write_buffer.extend_from_slice(b"-WRONGTYPE\r\n"),
            },
            None => write_buffer.extend_from_slice(b"*0\r\n"),
        }
    }

    // ----------------------------------------------------------
    // SCARD <key>
    // Returns the cardinality (number of members) in a set.
    // O(1) because HashSet::len() is constant time.
    // Returns :0 for missing keys.
    // ----------------------------------------------------------
    Command::Scard(key) => {
        let shard = db.read_shard(&key).await;
        match shard.get(&key) {
            Some(entry) => match &entry.value {
                crate::engine::DataType::Set(set) => {
                    write!(&mut write_buffer, ":{}\r\n", set.len()).unwrap();
                }
                _ => write_buffer.extend_from_slice(b"-WRONGTYPE\r\n"),
            },
            None => write_buffer.extend_from_slice(b":0\r\n"),
        }
    }

    // ----------------------------------------------------------
    // LLEN <key>
    // Returns the number of elements in a list.
    // Returns :0 for missing keys (a missing list has length 0).
    // ----------------------------------------------------------
    Command::Llen(key) => {
        let shard = db.read_shard(&key).await;
        match shard.get(&key) {
            Some(entry) => match &entry.value {
                crate::engine::DataType::List(list) => {
                    write!(&mut write_buffer, ":{}\r\n", list.len()).unwrap();
                }
                _ => write_buffer.extend_from_slice(b"-WRONGTYPE\r\n"),
            },
            None => write_buffer.extend_from_slice(b":0\r\n"),
        }
    }

    // ----------------------------------------------------------
    // HLEN <key>
    // Returns the number of fields in a hash.
    // Returns :0 for missing keys.
    // ----------------------------------------------------------
    Command::Hlen(key) => {
        let shard = db.read_shard(&key).await;
        match shard.get(&key) {
            Some(entry) => match &entry.value {
                crate::engine::DataType::Hash(hmap) => {
                    write!(&mut write_buffer, ":{}\r\n", hmap.len()).unwrap();
                }
                _ => write_buffer.extend_from_slice(b"-WRONGTYPE\r\n"),
            },
            None => write_buffer.extend_from_slice(b":0\r\n"),
        }
    }

    // ----------------------------------------------------------
    // SINTER <key_a> <key_b>
    // Returns the intersection of two sets — elements that appear
    // in BOTH sets.
    //
    // Algorithm:
    //   1. Clone both sets (to avoid holding locks across the computation)
    //   2. Use iterator filter with HashSet::contains for O(min(|A|,|B|))
    //      intersection (iterate the smaller set, check membership in the larger)
    //
    // Note: We clone the sets here because the multi-shard read lock
    // would otherwise be held for the duration of the intersection,
    // blocking other operations on those shards.
    //
    // Same cross-shard handling as MGET via read_multi_shards().
    // ----------------------------------------------------------
    Command::SInter(key_a, key_b) => {
        let mut set_a = std::collections::HashSet::<Bytes>::new();
        let mut set_b = std::collections::HashSet::<Bytes>::new();

        match db.read_multi_shards(&key_a, &key_b).await {
            MultiReadGuard::Single(shard) => {
                if let Some(e) = shard.get(&key_a) {
                    if let crate::engine::DataType::Set(s) = &e.value {
                        set_a = s.clone(); // Clone to release the lock after this block
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
        // Locks are released here as the match block ends.

        // Compute intersection: keep only elements that set_b also contains.
        // We iterate set_a and check membership in set_b (O(1) per check).
        let intersection: Vec<&Bytes> = set_a
            .iter()
            .filter(|item| set_b.contains(*item))
            .collect();

        write!(&mut write_buffer, "*{}\r\n", intersection.len()).unwrap();
        for item in intersection {
            write!(&mut write_buffer, "${}\r\n", item.len()).unwrap();
            write_buffer.extend_from_slice(item);
            write_buffer.extend_from_slice(b"\r\n");
        }
    }

    // ----------------------------------------------------------
    // KEYS <pattern>
    // Returns all keys in the database matching a glob-style pattern.
    //
    // Supported wildcards:
    //   * → matches any sequence of characters (converted to .* in regex)
    //   ? → matches any single character (converted to . in regex)
    //
    // WARNING: This is an O(N) operation over ALL keys in the database.
    // Redis itself warns against using KEYS in production on large
    // datasets. Prefer SCAN for iterative, cursor-based key discovery.
    //
    // Implementation:
    //   1. Convert the glob pattern to a full-match regex
    //   2. Collect all keys from all shards
    //   3. Filter by regex match
    // ----------------------------------------------------------
    Command::Keys(pattern) => {
        let pattern_str = bytes_to_str(&pattern);

        // Build a regex that anchors to the full key string (^ and $)
        // and replaces glob wildcards with regex equivalents.
        let regex_string = format!(
            "^{}$",
            pattern_str.replace("*", ".*").replace("?", ".")
        );

        let matcher = match regex::Regex::new(&regex_string) {
            Ok(re) => re,
            Err(_) => {
                // Invalid pattern (e.g., unbalanced brackets) → return error.
                write_buffer.extend_from_slice(b"-ERR invalid pattern format\r\n");
                return; // Exit the command handler entirely
            }
        };

        // get_all_keys() iterates all shards and collects every key.
        let all_keys = db.get_all_keys().await;

        // Filter to only keys whose UTF-8 representation matches the regex.
        // Keys that aren't valid UTF-8 are silently excluded (unlikely in practice).
        let filtered_keys: Vec<Bytes> = all_keys
            .into_iter()
            .filter(|key| {
                std::str::from_utf8(key)
                    .map(|s| matcher.is_match(s))
                    .unwrap_or(false)
            })
            .collect();

        // Emit as a RESP array of bulk strings.
        write!(&mut write_buffer, "*{}\r\n", filtered_keys.len()).unwrap();
        for key in &filtered_keys {
            write!(&mut write_buffer, "${}\r\n", key.len()).unwrap();
            write_buffer.extend_from_slice(key);
            write_buffer.extend_from_slice(b"\r\n");
        }
    }

    // ----------------------------------------------------------
    // SCAN <cursor> [MATCH <pattern>]
    // Incrementally iterates over keys using a cursor.
    // Unlike KEYS, SCAN is safe to use in production because it
    // only returns a small batch of keys per call.
    //
    // How this cursor scheme works:
    //   - The database has 64 shards (indices 0–63)
    //   - The cursor IS the shard index
    //   - Each SCAN call returns keys from ONE shard
    //   - The server returns the NEXT shard index as the new cursor
    //   - When cursor wraps back to 0, the full scan is complete
    //
    // RESP response format:
    //   *2\r\n
    //   $<cursor_len>\r\n<next_cursor>\r\n   ← next cursor value
    //   *<count>\r\n<key1><key2>...          ← keys from this batch
    //
    // When cursor == 64, the scan is done; return cursor=0 and empty array.
    // ----------------------------------------------------------
    Command::Scan(cursor, match_pattern) => {
        if cursor >= 64 {
            // Sentinel: cursor has wrapped past all shards.
            // Return cursor=0 to signal completion to the client.
            write_buffer.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
        } else {
            // Fetch all keys from shard at index `cursor`.
            let mut keys = db.scan_shard(cursor).await;

            // Optionally filter by MATCH pattern, same regex approach as KEYS.
            if let Some(pattern) = match_pattern {
                let pattern_str = bytes_to_str(&pattern);
                let regex_string = format!(
                    "^{}$",
                    pattern_str.replace("*", ".*").replace("?", ".")
                );
                if let Ok(matcher) = regex::Regex::new(&regex_string) {
                    keys.retain(|key| {
                        std::str::from_utf8(key)
                            .map(|s| matcher.is_match(s))
                            .unwrap_or(false)
                    });
                }
            }

            // Advance cursor to next shard, wrapping to 0 after shard 63.
            let next_cursor = if cursor == 63 { 0 } else { cursor + 1 };
            let next_cursor_str = next_cursor.to_string();

            write!(
                &mut write_buffer,
                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                next_cursor_str.len(),
                next_cursor_str,
                keys.len()
            )
            .unwrap();

            for key in &keys {
                write!(&mut write_buffer, "${}\r\n", key.len()).unwrap();
                write_buffer.extend_from_slice(key);
                write_buffer.extend_from_slice(b"\r\n");
            }
        }
    }

    // ----------------------------------------------------------
    // PUBLISH <channel> <message>
    // Publishes a message to all subscribers of a channel.
    //
    // This is NOT a request-response command from the publisher's
    // perspective — the publisher gets back an integer count of
    // how many subscribers received the message (handled inside
    // handle_publish). But first we flush any buffered writes to
    // ensure proper ordering.
    //
    // We flush write_buffer before entering the pubsub subsystem
    // because handle_publish may write directly to the stream,
    // and we don't want a partial write_buffer to interleave.
    // ----------------------------------------------------------
    Command::Publish(channel, message) => {
        // Flush any pending responses before handing off to pubsub.
        if !write_buffer.is_empty() {
            let _ = stream.write_all(&write_buffer).await;
            write_buffer.clear();
        }
        crate::pubsub::handle_publish(
            &pubsub,
            bytes_to_str(&channel),
            bytes_to_str(&message),
            &mut stream,
        )
        .await;
    }

    // ----------------------------------------------------------
    // SUBSCRIBE <channel>
    // Puts the client into pub/sub subscriber mode.
    //
    // Once subscribed, a client enters a special mode where:
    //   - It can ONLY send SUBSCRIBE/UNSUBSCRIBE/PING/QUIT
    //   - It receives pushed messages from the server asynchronously
    //   - The connection is no longer request-response
    //
    // handle_subscribe() takes over the connection loop for this
    // client. When it returns (e.g., on disconnect), we `break`
    // out of the main command loop entirely.
    //
    // We flush write_buffer first for the same reason as PUBLISH.
    // ----------------------------------------------------------
    Command::Subscribe(channel) => {
        if !write_buffer.is_empty() {
            let _ = stream.write_all(&write_buffer).await;
            write_buffer.clear();
        }
        // handle_subscribe blocks the task until the client unsubscribes
        // or disconnects. It takes ownership of the reader to receive
        // commands (like UNSUBSCRIBE) while in subscriber mode.
        crate::pubsub::handle_subscribe(
            &pubsub,
            bytes_to_str(&channel),
            &mut stream,
            &mut reader,
        )
        .await;

        // After subscribe mode ends, exit the command loop.
        // The connection cleanup code (if any) runs after this break.
        break;
    }

    // ----------------------------------------------------------
    // UNSUBSCRIBE <channel>
    // Removes a subscription from a channel.
    //
    // In this implementation, UNSUBSCRIBE is handled as a simple
    // acknowledgment (+OK). The actual unsubscription logic lives
    // inside handle_subscribe(), which monitors the connection and
    // processes UNSUBSCRIBE commands received while in subscriber mode.
    //
    // A client sending UNSUBSCRIBE outside of subscriber mode
    // (which is technically invalid) just gets +OK here.
    // ----------------------------------------------------------
    Command::Unsubscribe(channel) => {
        crate::log_debug!(
            "PubSub",
            "Client unsubscribed from: {}",
            bytes_to_str(&channel)
        );
        write_buffer.extend_from_slice(b"+OK\r\n");
    }

    // ----------------------------------------------------------
    // MONITOR
    // Puts the server into monitor mode for this connection.
    // The client receives a real-time stream of every command
    // processed by the server, prefixed with a timestamp.
    //
    // This is implemented using a Tokio broadcast channel (db.tx):
    //   - Every processed command is broadcast to the channel
    //   - The MONITOR client subscribes (rx.recv()) and relays them
    //   - This loop runs until the client disconnects or the channel errors
    //
    // We flush and break after the monitor loop ends, because
    // the connection is now in a degraded state (or the client left).
    // ----------------------------------------------------------
    Command::Monitor => {
        // Acknowledge that monitor mode is being entered.
        write_buffer.extend_from_slice(b"+OK\r\n");
        if stream.write_all(&write_buffer).await.is_err() {
            break; // Client disconnected before we even started monitoring
        }
        write_buffer.clear();

        // Subscribe to the broadcast channel that receives all
        // processed command strings from the server.
        let mut rx = db.tx.subscribe();
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    // Forward each command as a simple string to the client.
                    // Format: +<timestamp> [db] "command arg1 arg2"\r\n
                    // (simplified here — no timestamp or db prefix)
                    let response = format!("+{}\r\n", msg);
                    if stream.write_all(response.as_bytes()).await.is_err() {
                        break; // Client disconnected — exit monitor loop
                    }
                }
                Err(_) => {
                    // Channel lagged or was closed (e.g., server shutting down).
                    break;
                }
            }
        }
        // Exit the outer command loop after monitor mode ends.
        break;
    }

    // ----------------------------------------------------------
    // CATCH-ALL
    // Any command that reaches here was parsed successfully but has
    // no handler implemented. This could be a valid Redis command
    // we haven't built yet (e.g., ZADD, EXPIRE, OBJECT, etc.)
    //
    // We return a generic error response to the client.
    // ----------------------------------------------------------
    _ => {
        write_buffer.extend_from_slice(b"-ERR unknown command\r\n");
    }
}

        // 5. FINALIZE RESPONSE
        if !write_buffer.is_empty() {
            let _ = stream.write_all(&write_buffer).await;
            write_buffer.clear();
        }
    }
}

/// Start the TCP Listener and accept loop.
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
                let pass_handle = shared_password.clone();

                let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                {
                    let mut clients = active_clients.lock().unwrap();
                    clients.insert(socket_addr, ClientInfo {
                        id: client_id,
                        addr: socket_addr,
                        connected_at: SystemTime::now(),
                    });
                }

                // Spawn a lightweight Tokio task for the client.
                // Titan KV handles thousands of concurrent tasks efficiently via work-stealing.
                tokio::spawn(async move {
                    handle_connection(stream, db_handle, pubsub_handle, socket_addr, clients_handle, pass_handle).await;
                });
            }
            Err(e) => {
                crate::log_error!("Server", "Connection Failed: {}", e);
            }
        };
    }
}
