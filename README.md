```
 ████████╗██╗████████╗ █████╗ ███╗   ██╗    ██╗  ██╗██╗   ██╗
    ██╔══╝██║╚══██╔══╝██╔══██╗████╗  ██║    ██║ ██╔╝██║   ██║
    ██║   ██║   ██║   ███████║██╔██╗ ██║    █████╔╝ ██║   ██║
    ██║   ██║   ██║   ██╔══██║██║╚██╗██║    ██╔═██╗ ╚██╗ ██╔╝
    ██║   ██║   ██║   ██║  ██║██║ ╚████║    ██║  ██╗ ╚████╔╝
    ╚═╝   ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝    ╚═╝  ╚═╝  ╚═══╝
```

<div align="center">

**A Redis-compatible, in-memory key-value store — built from scratch in Rust.**

[![Rust](https://img.shields.io/badge/Rust-2024_Edition-orange?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Async-Tokio-purple?style=flat-square)](https://tokio.rs/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker&logoColor=white)](Dockerfile)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active_Development-brightgreen?style=flat-square)]()

</div>

---

## What Is Titan KV?

Titan KV is a ground-up implementation of a Redis-compatible key-value store written entirely in safe Rust. It is not a wrapper, a binding, or a port — it is an original engine built to understand and replicate the core mechanics that make Redis one of the most battle-tested data stores in existence.

It speaks the Redis wire protocol on port `6379`, which means any Redis client — `redis-cli`, `nc`, your application's existing Redis library — can connect to it without modification.

The goal is not to beat Redis. The goal is to understand what it takes to build something like it: the concurrency model, the durability guarantees, the expiration mechanics, the persistence format. Every line of Titan KV exists as a deliberate engineering decision.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                            CLIENT                                │
│                (redis-cli / nc / any TCP client)                 │
└──────────────────────────────┬───────────────────────────────────┘
                               │ TCP :6379
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                          server.rs                               │
│   TcpListener → tokio::spawn per connection                      │
│   BufReader and tokio async Read/Write usage                     │
│   read_resp() → parse_command() → dispatch                       │
└───────────┬──────────────────────────────────────┬──────────────┘
            │                                      │
            ▼                                      ▼
┌───────────────────────┐       ┌──────────────────────────────────┐
│     protocol.rs       │       │    engine.rs  (ShardedDb)        │
│                       │       │                                  │
│  parse_command([Str]) │       │  Arc<[RwLock<HashMap             │
│                       │       │    <String,Entry>>; 64]>         │
│  SET / LISTS / HASH   │       │                                  │
│  PUBSUB / PING        │       │  shard_index = hash(key) & 0x3F  │
│  RPOPLPUSH / LREM     │       │                                  │
│  read_resp(reader)    │       │  ┌────┬────┬────┬─ … ─┬────┐    │
│                       │       │  │ S0 │ S1 │ S2 │     │S63 │    │
└───────────────────────┘       │  └────┴────┴────┴─ … ─┴────┘    │
                                │                                  │
                                │  Entry { value: DataType,        │
                                │    expires_at: Option<SystemTime>│
                                │  }                               │
                                └────────────────┬─────────────────┘
                                                 │
                             ┌───────────────────┼──────────────────┐
                             ▼                   ▼                  ▼
                      ┌──────────┐  ┌──────────────────┐  ┌──────────────┐
                      │ AOF Log  │  │   Expiration     │  │    AOF       │
                      │ Append   │  │   Sweeper        │  │  Compaction  │
                      │ on Write │  │ (10s · all shards│  │  (60s tick)  │
                      └──────────┘  └──────────────────┘  └──────────────┘
                             │                   │                  │
                             └───────────────────┴──────────────────┘
                                                 │
                                         ┌───────────────┐
                                         │ database.aof  │
                                         │  (on disk)    │
                                         └───────────────┘
```

---

## Core Components

### `engine.rs` — The Heart

The database type is `ShardedDb`: an `Arc`-wrapped array of **64 independent `RwLock<HashMap<String, Entry>>`** shards. When a command arrives for key `K`, the shard index is derived as `hash(K) & 0x3F` — directing every operation to exactly one shard without touching the other 63.

This means up to 64 concurrent writers can hold write guards simultaneously, as long as they are hashing to different shards. `RwLock` within each shard still allows unlimited concurrent readers per shard while serializing writes to that shard.

Each `Entry` carries a `value: DataType` (`String`, `List`, or `Hash`) and an `expires_at: Option<SystemTime>`. For cross-shard operations like `RPOPLPUSH`, `write_multi_shards` acquires both locks in index order — lowest index first — making deadlock structurally impossible.

### `server.rs` — The Listener

A Tokio `TcpListener` accepts connections in a tight loop. Each accepted connection is handed off to `tokio::spawn`, giving every client its own lightweight async task. The handler manages RESP input, executes commands against the sharded database, and serializes responses.

### `protocol.rs` — The Parser

A complete RESP-compatible command reader and parser. It tokenizes the incoming binary stream, supports arrays and bulk strings, and returns a strongly-typed `Command` enum encompassing scalar types, lists, hashes, pubsub operations, and the new queue primitives `RPOPLPUSH` and `LREM`.

### `pubsub.rs` — Event Broker

Adds real-time publish/subscribe functionality using a separate collection of Tokio broadcast channels. Clients who `SUBSCRIBE` pause typical command parsing to await asynchronous broadcast wakeups whenever another connection pushes a `PUBLISH` event on the channel.

### `logger.rs` — The Monitoring System

A custom macro-driven logging module utilizing `chrono` and `colored`. It replaces generic print statements with vivid, timestamped, and dynamically color-coded terminal outputs (`[INFO]`, `[ERR!]`, `[DBUG]`), ensuring precise tracking of connections, swept keys, incoming RESP traffic, and AOF state.

### `main.rs` — The Orchestrator

Startup, background tasks, and server launch live here:

1. **AOF Replay** — On boot, the existing `database.aof` is replayed line by line, routing each key to its correct shard to reconstruct in-memory state. The database is live again in milliseconds.
2. **Active Expiration Sweeper** — A background task wakes every 10 seconds, iterates across all 64 shards, removes expired keys, and appends `DEL` entries to the AOF.
3. **AOF Compaction** — Every 60 seconds, the current live state across all shards is serialized to a temporary file and atomically renamed over `database.aof`. This collapses redundant command history and keeps the log from growing unbounded.

### `thread_pool.rs` — The Handcrafted Executor

Before Tokio took over async dispatch, Titan KV shipped a fully hand-rolled OS thread pool using `std::sync::mpsc` channels and `Arc<Mutex<Receiver>>`. Workers block on channel recv and execute jobs; the `Drop` implementation drains the channel and joins all threads for a clean shutdown. It remains in the codebase as a foundational artifact — proof that the concurrency model was understood before it was abstracted away.

---

## Commands

| Command            | Syntax                    | Response                   | Notes                                                  |
| ------------------ | ------------------------- | -------------------------- | ------------------------------------------------------ |
| **Keys / General** |                           |                            |                                                        |
| `PING`             | `PING`                    | `+PONG`                    | Connection health check                                |
| `SET`              | `SET key value`           | `+OK`                      | Write to shard + AOF                                   |
| `GET`              | `GET key`                 | `$<len>\r\n<val>` or `$-1` | Lazy expiry check on read                              |
| `MGET`             | `MGET key1 key2 ...`      | `*<count>\r\n...`          | Get multiple keys                                      |
| `DEL`              | `DEL key`                 | `:1` or `:0`               | Removes key from shard + AOF entry                     |
| `EXISTS`           | `EXISTS key`              | `:1` or `:0`               | Read-only, acquires shard read guard                   |
| `INCR`             | `INCR key`                | `:<new_value>` or `-ERR`   | Atomic integer increment within shard                  |
| `SETEX`            | `SETEX key seconds value` | `+OK`                      | Write with TTL; expiry stored as `SystemTime`          |
| `TYPE`             | `TYPE key`                | `+<type>`                  | Return the type of value stored                        |
| `TTL` / `PTTL`     | `TTL key`                 | `:<seconds>` or `:-2`      | Returns remaining time to live                         |
| `KEYS`             | `KEYS pattern`            | `*<count>\r\n...`          | Find all keys matching given pattern                   |
| `SCAN`             | `SCAN cursor [MATCH pat]` | `*2\r\n...`                | Incrementally iterate keyspace                         |
| `STRLEN`           | `STRLEN key`              | `:<len>`                   | Get length of string value                             |
| **Lists**          |                           |                            |                                                        |
| `LPUSH`            | `LPUSH key value`         | `:<length>`                | Push to head of list (`VecDeque::push_front`)          |
| `RPUSH`            | `RPUSH key value`         | `:<length>`                | Push to tail of list (`VecDeque::push_back`)           |
| `LPOP`             | `LPOP key`                | `$<len>\r\n<val>`          | Pop from head of list                                  |
| `RPOP`             | `RPOP key`                | `$<len>\r\n<val>`          | Pop from tail of list                                  |
| `RPOPLPUSH`        | `RPOPLPUSH source dest`   | `$<len>\r\n<val>` or `$-1` | Atomic tail-pop → head-push; cross-shard deadlock-safe |
| `LREM`             | `LREM key count value`    | `:1` or `:0`               | Remove first matching element from list                |
| `LTRIM`            | `LTRIM key start stop`    | `+OK`                      | Trim list to range; supports negative indices          |
| `LRANGE`           | `LRANGE key start stop`   | `*<count>\r\n...`          | Get range of elements; supports negative indices       |
| `LLEN`             | `LLEN key`                | `:<len>`                   | Gets the length of a list                              |
| **Hashes**         |                           |                            |                                                        |
| `HSET`             | `HSET key field value`    | `+OK`                      | Add or update a field in a hash                        |
| `HGET`             | `HGET key field`          | `$<len>\r\n<val>` or `$-1` | Get the value of a field                               |
| `HGETALL`          | `HGETALL key`             | `*<count>\r\n...`          | Retrieve all fields and values from a hash             |
| `HLEN`             | `HLEN key`                | `:<len>`                   | Get number of fields in a hash                         |
| **Sets**           |                           |                            |                                                        |
| `SADD`             | `SADD key member`         | `:1` or `:0`               | Add member to a set                                    |
| `SMEMBERS`         | `SMEMBERS key`            | `*<count>\r\n...`          | Get all members of a set                               |
| `SCARD`            | `SCARD key`               | `:<len>`                   | Get number of members in a set                         |
| `SINTER`           | `SINTER key1 key2`        | `*<count>\r\n...`          | Intersect multiple sets                                |
| **PubSub**         |                           |                            |                                                        |
| `PUBLISH`          | `PUBLISH channel msg`     | `:<receivers>`             | Push a message into the event bus                      |
| `SUBSCRIBE`        | `SUBSCRIBE channel`       | `*3\r\n...`                | Listen for broadcasts indefinitely                     |
| `UNSUBSCRIBE`      | `UNSUBSCRIBE channel`     | `*3\r\n...`                | End a subscription                                     |
| **Server**         |                           |                            |                                                        |
| `INFO`             | `INFO`                    | `$<len>\r\n...`            | Get server information and stats                       |
| `DBSIZE`           | `DBSIZE`                  | `:<len>`                   | Return number of keys in the selected DB               |
| `MONITOR`          | `MONITOR`                 | `+OK`                      | Stream all processed commands in real-time             |
| `CLIENT LIST`      | `CLIENT LIST`             | `$<len>\r\n...`            | Get list of client connections                         |
| `MEMORY USAGE`     | `MEMORY USAGE key`        | `:<bytes>`                 | Estimate memory used by key                            |

---

## Durability: The AOF Model

Titan KV uses an **Append-Only File** for persistence — the same model Redis uses in its `appendonly yes` mode.

**On every mutating command** (`SET`, `DEL`, `INCR`, `SETEX`, `LPUSH`, `RPUSH`, `RPOPLPUSH`, etc.), the command is serialized to `database.aof` _before_ the response is sent. If the process dies at any moment, the on-disk log is authoritative.

**On startup**, the AOF is replayed top-to-bottom. Each command is re-executed against the sharded engine, routing keys to their correct shards. The in-memory state is deterministically reconstructed. There is no binary snapshot to corrupt, no partial-write ambiguity.

**Every 60 seconds**, the compaction task iterates all 64 shards, rewrites the AOF to reflect only the current live state, and atomically renames it into place. This collapses redundant command history and prevents unbounded log growth.

```
Timeline:

  t=0   SET username alice          → AOF: "SET username alice"
  t=5   SET username bob            → AOF: "SET username alice\nSET username bob"
  t=10  SET username charlie        → AOF: "SET username alice\nSET username bob\nSET username charlie"
  t=60  [compaction fires]          → AOF: "SET username charlie"   ← 3 lines → 1
```

---

## Expiration: Two Strategies in Tandem

Titan KV implements the same dual-expiry pattern Redis uses:

**1. Lazy Expiration** — When `GET` is called on a key, the handler checks `expires_at` against `SystemTime::now()`. If the key is stale, it is deleted from its shard, a `DEL` is appended to the AOF, and `$-1` (nil) is returned. No background task needed for this path.

**2. Active Expiration** — A background sweeper wakes every 10 seconds. It iterates across all 64 shards, acquires each write lock in turn, collects the expired keys, removes them in batch, and flushes `DEL` entries to the AOF. This prevents keys that are never read from living forever in memory.

```
SETEX session:abc 30 "user_data"

  t+0s    GET session:abc   → "+user_data"     (fresh)
  t+31s   GET session:abc   → "$-1"            (lazy expiry fires)
  t+10s   [sweeper runs]    → any unread expired keys evicted across all shards
```

---

## Concurrency Model

Every client connection is an independent Tokio task. The shared state is a `ShardedDb` — an array of **64 independent `RwLock` shards**, each protecting a separate `HashMap<String, Entry>`.

Keys are routed to shards via a 64-bit hash masked to 6 bits (`hash(key) & 0x3F`). This means:

- **N concurrent readers per shard** — `GET`, `EXISTS`, `LRANGE`, `HGET`, `HGETALL` acquire a read guard on the relevant shard, never blocking reads on other shards.
- **Up to 64 concurrent writers** — `SET`, `DEL`, `INCR`, `LPUSH`, etc. acquire a write guard on exactly one shard. Writes to different shards proceed in parallel with zero contention.
- **Cross-shard atomicity** — `RPOPLPUSH` may span two shards. `write_multi_shards` always acquires locks in ascending index order (lower index first), making deadlock structurally impossible regardless of key ordering.

```
hash("queue:jobs")   & 0x3F → shard 17
hash("queue:done")   & 0x3F → shard 42
hash("session:abc")  & 0x3F → shard 5

RPOPLPUSH "queue:jobs" "queue:done"
  → acquire shard 17 write guard
  → acquire shard 42 write guard  (17 < 42, safe ordering)
  → pop tail of queue:jobs
  → push head of queue:done
  → release both guards atomically
```

This scales naturally: a workload running 64 concurrent writers on distinct key spaces has zero lock contention.

---

## Job Queue Pattern

`RPOPLPUSH` enables a reliable, Redis-style job queue pattern:

```bash
# Producer — enqueue jobs
redis-cli -p 6379 RPUSH jobs:pending "task:1"
redis-cli -p 6379 RPUSH jobs:pending "task:2"

# Worker — atomically move job from pending → processing
redis-cli -p 6379 RPOPLPUSH jobs:pending jobs:processing
# → "task:1"

# On success — remove from processing
redis-cli -p 6379 LREM jobs:processing 1 "task:1"

# On crash — jobs:processing still contains task:1
# Recover: RPOPLPUSH jobs:processing jobs:pending
```

Because `RPOPLPUSH` is atomic — even when source and destination are in different shards — a job is never lost between the pop and the push. The processing list acts as an in-flight registry that survives worker crashes.

---

## Getting Started

**Prerequisites:** Rust toolchain (edition 2024), Cargo, Tokio.

```bash
# Clone
git clone https://github.com/yourname/titan_kv
cd titan_kv

# Build & run
cargo run --release

# In another terminal — use redis-cli
redis-cli -p 6379 PING
# → PONG

redis-cli -p 6379 SET planet "Earth"
# → OK

redis-cli -p 6379 GET planet
# → "Earth"

redis-cli -p 6379 SETEX token 60 "abc123"
redis-cli -p 6379 EXISTS token
# → (integer) 1

redis-cli -p 6379 INCR visits
# → (integer) 1

redis-cli -p 6379 INCR visits
# → (integer) 2

# Job queue
redis-cli -p 6379 RPUSH queue "job1"
redis-cli -p 6379 RPOPLPUSH queue processing
# → "job1"
redis-cli -p 6379 LREM processing 1 "job1"
# → (integer) 1
```

Or, without redis-cli:

```bash
# Raw TCP via netcat
echo -e "SET name Titan\n" | nc 127.0.0.1 6379
# → +OK

echo -e "GET name\n" | nc 127.0.0.1 6379
# → $5\r\nTitan\r\n
```

---

## Docker

Titan KV ships with a multi-stage `Dockerfile`. The build stage compiles the release binary inside `rust:1.81-slim-bookworm`; the runtime stage copies only the resulting binary into a minimal `debian:bookworm-slim` image — no Rust toolchain, no source code, no intermediate artifacts.

```bash
# Build the image
docker build -t titan_kv .

# Run — maps the standard Redis port
docker run -p 6379:6379 titan_kv

# Run with a persistent AOF volume so data survives container restarts
docker run -p 6379:6379 -v $(pwd)/data:/app titan_kv
```

Connect from the host exactly as you would with a local instance:

```bash
redis-cli -p 6379 PING
# → PONG
```

---

## On Safety

Titan KV contains **zero `unsafe` blocks**. The Rust compiler's ownership model guarantees:

- No data races across async tasks (enforced at compile time by `Send + Sync` bounds)
- No use-after-free (ownership transfer, not raw pointers)
- No buffer overflows (the `take` guard is enforced at the type level)
- No null pointer dereferences (`Option<T>` instead of null)
- No deadlocks in cross-shard operations (ordered lock acquisition is a structural guarantee, not a runtime check)

The AOF compaction uses an atomic rename (`temp → live`) rather than truncate-in-place, making it crash-safe at the OS level.

---

## What's Next

---

### The Stress Test — Benchmarking Branch

> _How fast is fast?_

A dedicated benchmarking branch using `redis-benchmark` (or a custom Rust harness) to blast the server with 100,000+ requests and measure raw operations per second. With sharding in place, the expected bottleneck is no longer the global lock — the goal is to identify whether AOF fsync or Tokio scheduling is the new ceiling.

```bash
redis-benchmark -p 6379 -n 100000 -c 50 -t set,get
```

---

### Advanced Data Structures (In-Progress)

> _Strings, Lists, Hashes, Sets, and beyond._

With String, `VecDeque`-backed List, `HashMap`-backed Hash, and newly introduced `HashSet`-backed Sets already in place, the engine's `DataType` enum continues to expand:

- **Sorted Sets** — `ZADD`, `ZRANGE`, `ZSCORE` to support leaderboard-like operations

---

### `titan-cli` — A Native Client

> _A tool worthy of the server._

A companion binary — `titan-cli` — that replaces `redis-cli` and `nc` with a purpose-built interactive prompt:

- REPL with readline history and tab completion
- Pretty-printed responses (tables for hashes, numbered lists for arrays)
- Connection health indicator in the prompt
- Pipe-mode for scripting: `echo "GET key" | titan-cli`

Built as a second binary in the same Cargo workspace, sharing the protocol module.

---

## Project Layout

```
titan_kv/
├── src/
│   ├── main.rs          # Startup: AOF replay, background tasks, server launch
│   ├── server.rs        # TCP listener, per-connection async handler
│   ├── protocol.rs      # RESP parser → typed Command enum
│   ├── engine.rs        # ShardedDb: 64-shard Arc<RwLock<HashMap<String, Entry>>>
│   ├── logger.rs        # Advanced color-coded terminal monitoring macros
│   ├── pubsub.rs        # Broadcast broker logic managing subscription channels
│   └── thread_pool.rs   # Hand-rolled OS thread pool (graceful shutdown)
├── Dockerfile           # Multi-stage build: rust:1.81-slim → debian:bookworm-slim
├── database.aof         # Append-only log (created on first write)
└── Cargo.toml
```

---

## Design Influences

- **Redis** — The wire protocol, the AOF persistence model, the dual expiration strategy, the command semantics, and the `RPOPLPUSH` job-queue pattern are all faithful to Redis's documented behavior.
- **"The Rust Programming Language" Book** — The thread pool in `thread_pool.rs` is a direct evolution of the Chapter 20 project, extended with production-aware graceful shutdown.
- **Tokio** — The async runtime, the `RwLock`, and the `spawn`-per-connection model follow Tokio's recommended patterns for I/O-bound concurrent servers.

---

<div align="center">

_Built in Rust. Built to learn. Built to last._

</div>
