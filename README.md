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
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT                               │
│              (redis-cli / nc / any TCP client)              │
└───────────────────────────┬─────────────────────────────────┘
                            │ TCP :6379
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     server.rs                               │
│   TcpListener → tokio::spawn per connection                 │
│   BufReader and tokio async Read/Write usage                │
│   read_resp() → parse_command() → dispatch                  │
└───────────┬─────────────────────────────────┬───────────────┘
            │                                 │
            ▼                                 ▼
┌───────────────────────┐       ┌─────────────────────────────┐
│     protocol.rs       │       │         engine.rs           │
│                       │       │                             │
│  parse_command([Str]) │       │  Arc<RwLock<HashMap         │
│                       │       │    <String, Entry>>>        │
│  SET / LISTS / HASH   │       │                             │
│  PUBSUB / PING        │       │  Entry {                    │
│  read_resp(reader)    │       │    value: DataType,         │
│                       │       │    expires_at: Option       │
└───────────────────────┘       │      <SystemTime>           │
                                │  }                          │
                                └─────────────────────────────┘
                                            │
                            ┌───────────────┼───────────────┐
                            ▼               ▼               ▼
                     ┌──────────┐  ┌──────────────┐  ┌──────────────┐
                     │ AOF Log  │  │  Expiration  │  │    AOF       │
                     │ Append   │  │  Sweeper     │  │  Compaction  │
                     │ on Write │  │  (10s tick)  │  │  (60s tick)  │
                     └──────────┘  └──────────────┘  └──────────────┘
                            │               │               │
                            └───────────────┴───────────────┘
                                            │
                                    ┌───────────────┐
                                    │ database.aof  │
                                    │  (on disk)    │
                                    └───────────────┘
```

---

## Core Components

### `engine.rs` — The Heart

The database is a single type: `Arc<RwLock<HashMap<String, Entry>>>`. `Arc` enables safe shared ownership across async tasks and connection handlers. `RwLock` (from Tokio) allows unlimited concurrent readers while serializing writes — because most workloads read far more than they write.

Each `Entry` carries a `value: DataType` (which models natively supported types like `String`, `List`, and `HashMap`) and an `expires_at: Option<SystemTime>`. Expiration is a first-class citizen in the data model, not a bolted-on afterthought.

### `server.rs` — The Listener

A Tokio `TcpListener` accepts connections in a tight loop. Each accepted connection is handed off to `tokio::spawn`, giving every client its own lightweight async task. The handler manages RESP input, executes the commands on the global memory state, and serializes responses.

### `protocol.rs` — The Parser

A complete RESP-compatible command reader and parser. It tokenizes the incoming binary stream, supports arrays and bulk strings, and returns a strongly-typed `Command` enum encompassing scalar types, lists, hashes, and pubsub operations.

### `pubsub.rs` — Event Broker

Adds real-time publish/subscribe functionality using a separate collection of Tokio broadcast channels. Clients who `SUBSCRIBE` pause typical command parsing to await asynchronous broadcast wakeups whenever another connection pushes a `PUBLISH` event on the channel.

### `logger.rs` — The Monitoring System

A custom macro-driven logging module utilizing `chrono` and `colored`. It replaces generic print statements with vivid, timestamped, and dynamically color-coded terminal outputs (`[INFO]`, `[ERR!]`, `[DBUG]`), ensuring precise tracking of connections, swept keys, incoming RESP traffic, and AOF state.

### `main.rs` — The Orchestrator

Startup, background tasks, and server launch live here:

1. **AOF Replay** — On boot, the existing `database.aof` is replayed line by line to reconstruct in-memory state. The database is live again in milliseconds.
2. **Active Expiration Sweeper** — A background task wakes every 10 seconds, scans for expired keys, removes them from memory, and appends `DEL` entries to the AOF.
3. **AOF Compaction** — Every 60 seconds, the current live state is serialized to a temporary file and atomically renamed over `database.aof`. This collapses redundant command history (a key set and overridden 1000 times becomes a single `SET`) and keeps the log from growing unbounded.

### `thread_pool.rs` — The Handcrafted Executor

Before Tokio took over async dispatch, Titan KV shipped a fully hand-rolled OS thread pool using `std::sync::mpsc` channels and `Arc<Mutex<Receiver>>`. Workers block on channel recv and execute jobs; the `Drop` implementation drains the channel and joins all threads for a clean shutdown. It remains in the codebase as a foundational artifact — proof that the concurrency model was understood before it was abstracted away.

---

## Commands

| Command       | Syntax                    | Response                   | Notes                                           |
| ------------- | ------------------------- | -------------------------- | ----------------------------------------------- |
| **Keys**      |                           |                            |                                                 |
| `PING`        | `PING`                    | `+PONG`                    | Connection health check                         |
| `SET`         | `SET key value`           | `+OK`                      | Write to memory + AOF                           |
| `GET`         | `GET key`                 | `$<len>\r\n<val>` or `$-1` | Lazy expiry check on read                       |
| `DEL`         | `DEL key`                 | `:1` or `:0`               | Removes key + AOF entry                         |
| `EXISTS`      | `EXISTS key`              | `:1` or `:0`               | Read-only, uses RwLock read guard               |
| `INCR`        | `INCR key`                | `:<new_value>` or `-ERR`   | Atomic integer increment; errors on non-integer |
| `SETEX`       | `SETEX key seconds value` | `+OK`                      | Write with TTL; expiry stored as `SystemTime`   |
| **Lists**     |                           |                            |                                                 |
| `LPUSH`       | `LPUSH key value`         | `:<length>`                | Push an element to the head of a list           |
| `RPUSH`       | `RPUSH key value`         | `:<length>`                | Push an element to the tail of a list           |
| `LPOP`        | `LPOP key`                | `$<len>\r\n<val>`          | Pop an element from the head of a list          |
| **Hashes**    |                           |                            |                                                 |
| `HSET`        | `HSET key fld val`        | `+OK`                      | Add or update a field in a hash map             |
| `HGET`        | `HGET key field`          | `$<len>\r\n<val>` or `$-1` | Get the value of a field in a hash map          |
| `HGETALL`     | `HGETALL key`             | `*<count>\r\n...`          | Retrieve all fields and values from a hash map  |
| **PubSub**    |                           |                            |                                                 |
| `PUBLISH`     | `PUBLISH channel msg`     | `:<receivers>`             | Push a message into the event bus               |
| `SUBSCRIBE`   | `SUBSCRIBE channel`       | `*3\r\n...`                | Listen for broadcasts indefinitely              |
| `UNSUBSCRIBE` | `UNSUBSCRIBE ch`          | `*3\r\n...`                | End a subscription                              |

---

## Durability: The AOF Model

Titan KV uses an **Append-Only File** for persistence — the same model Redis uses in its `appendonly yes` mode.

**On every mutating command** (`SET`, `DEL`, `INCR`, `SETEX`), the command is serialized to `database.aof` _before_ the response is sent. If the process dies at any moment, the on-disk log is authoritative.

**On startup**, the AOF is replayed top-to-bottom. The in-memory state is deterministically reconstructed from the command log. There is no binary snapshot to corrupt, no partial-write ambiguity.

**Every 60 seconds**, the compaction task rewrites the AOF to reflect only the current live state — eliminating tombstoned deletes, expired keys, and overwritten values. The rewrite goes to `database.temp.aof` and is atomically renamed into place, so a crash mid-compaction never corrupts the live log.

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

**1. Lazy Expiration** — When `GET` is called on a key, the handler checks `expires_at` against `SystemTime::now()`. If the key is stale, it is deleted, a `DEL` is appended to the AOF, and `$-1` (nil) is returned. No background task needed for this path.

**2. Active Expiration** — A background sweeper wakes every 10 seconds. It acquires a write lock, iterates all keys, and collects the expired ones. Expired keys are removed in batch, and their `DEL` entries are flushed to the AOF. This prevents keys that are never read from living forever in memory.

```
SETEX session:abc 30 "user_data"

  t+0s    GET session:abc   → "+user_data"     (fresh)
  t+31s   GET session:abc   → "$-1"            (lazy expiry fires)
  t+10s   [sweeper runs]    → any unread expired keys evicted
```

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

## Concurrency Model

Every client connection is an independent Tokio task. The shared state is a single `Arc<RwLock<...>>` — no per-shard locks, no sharding, no lock-free tricks yet. The Tokio `RwLock` allows:

- **N concurrent readers** — `GET`, `EXISTS` acquire a read guard that does not block other reads.
- **1 exclusive writer** — `SET`, `DEL`, `INCR`, `SETEX` acquire a write guard that waits for all readers to finish.

This is correct, safe, and sufficient. It is also the natural starting point for future optimization: the bottleneck under high write contention is the single global lock — which is exactly what sharded hashmaps and lock-free structures are designed to address.

---

## On Safety

Titan KV contains **zero `unsafe` blocks**. The Rust compiler's ownership model guarantees:

- No data races across async tasks (enforced at compile time by `Send + Sync` bounds)
- No use-after-free (ownership transfer, not raw pointers)
- No buffer overflows (the 1024-byte `take` guard is enforced at the type level)
- No null pointer dereferences (`Option<T>` instead of null)

The AOF compaction uses an atomic rename (`temp → live`) rather than truncate-in-place, making it crash-safe at the OS level.

---

## What's Next

These are the next major milestones for Titan KV, each targeting a distinct dimension of what makes a production-grade data store:

---

### The Stress Test — Benchmarking Branch

> _How fast is fast?_

A dedicated benchmarking branch using `redis-benchmark` (or a custom Rust harness) to blast the server with 100,000+ requests and measure raw operations per second. The goal is to establish a baseline, identify the actual bottleneck (is it the global lock? the AOF fsync? the Tokio scheduler?), and document throughput under realistic concurrent load.

```bash
# What this will look like:
redis-benchmark -p 6379 -n 100000 -c 50 -t set,get
```

---

### Advanced Data Structures & Capabilities

> _Strings, Lists, Hashmaps, and beyond._

With core scalar data, `VecDeque`-backed lists, and nested `HashMap` instances already implemented, the engine's Enum model could further encompass:

- **Sets** — `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER` backed by a `HashSet<String>`
- **Sorted Sets** — `ZADD`, `ZRANGE`, `ZSCORE` to support complex leaderboard-like operations.

---

### `titan-cli` — A Native Client

> _A tool worthy of the server._

A companion binary — `titan-cli` — that replaces `redis-cli` and `nc` with a purpose-built interactive prompt. Features planned:

- REPL with readline history and tab completion
- Pretty-printed responses (tables for hash maps, numbered lists for arrays)
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
│   ├── engine.rs        # Core DB type: Arc<RwLock<HashMap<String, Entry>>>
│   ├── logger.rs        # Advanced color-coded terminal monitoring macros
│   ├── pubsub.rs        # Broadcast broker logic managing subscription channels
│   └── thread_pool.rs   # Hand-rolled OS thread pool (graceful shutdown)
├── database.aof         # Append-only log (created on first write)
└── Cargo.toml
```

---

## Design Influences

- **Redis** — The wire protocol, the AOF persistence model, the dual expiration strategy, and the command semantics are all faithful to Redis's documented behavior.
- **"The Rust Programming Language" Book** — The thread pool in `thread_pool.rs` is a direct evolution of the Chapter 20 project, extended with production-aware graceful shutdown.
- **Tokio** — The async runtime, the `RwLock`, and the `spawn`-per-connection model follow Tokio's recommended patterns for I/O-bound concurrent servers.

---

<div align="center">

_Built in Rust. Built to learn. Built to last._

</div>
