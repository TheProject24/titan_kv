// src/engine.rs
//! Titan KV Core Engine
//! 
//! This module implements the sharded storage engine. 
//! To avoid global lock contention (the "Redis single-thread bottleneck"), we partition the 
//! keyspace into 64 independent shards, each protected by its own asynchronous RwLock.

use std::collections::{ HashMap, HashSet, VecDeque };
use std::hash::{ Hash, Hasher };
use std::collections::hash_map::DefaultHasher;
use tokio::sync::{ broadcast, RwLock, RwLockReadGuard, RwLockWriteGuard, mpsc };
use std::sync::Arc;
use std::time::SystemTime;
use bytes::Bytes;

/// Convenience alias for the sharded database handle.
pub type Db = ShardedDb;

/// Entry point for creating a new database instance.
/// Returns the shared DB handle and a receiver for the Append-Only-File (AOF) stream.
pub fn new_db() -> (Db, mpsc::Receiver<String>) {
    ShardedDb::new_db()
}

/// Helper enum for holding multiple write guards across different shards.
/// This allows us to perform atomic cross-shard operations (like RPOPLPUSH).
pub enum MultiWriteGuard<'a> {
    Single(RwLockWriteGuard<'a, HashMap<Bytes, Entry>>),
    Double(
        RwLockWriteGuard<'a, HashMap<Bytes, Entry>>,
        RwLockWriteGuard<'a, HashMap<Bytes, Entry>>,
    ),
}

/// Helper enum for holding multiple read guards.
pub enum MultiReadGuard<'a> {
    Single(RwLockReadGuard<'a, HashMap<Bytes, Entry>>),
    Double(
        RwLockReadGuard<'a, HashMap<Bytes, Entry>>,
        RwLockReadGuard<'a, HashMap<Bytes, Entry>>,
    ),
}

// 64 shards is a sweet spot: enough to minimize contention for thousands of clients,
// but few enough that the background sweeper (TTL) doesn't over-burden the CPU.
const SHARD_COUNT: usize = 64;
const SHARD_MASK: u64 = (SHARD_COUNT - 1) as u64;

/// Supported Data Types in Titan KV.
#[derive(Clone, Debug)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
}

/// A single entry in the database.
#[derive(Debug)]
pub struct Entry {
    pub value: DataType,
    /// Absolute expiration time. If None, the key lives forever.
    pub expires_at: Option<SystemTime>,
}

/// The internal sharded database structure.
/// Data is stored in 64 HashMaps, each behind a Tokio RwLock.
pub struct ShardedDb {
    shards: Arc<[RwLock<HashMap<Bytes, Entry>>; SHARD_COUNT]>,
    /// Broadcast channel for the MONITOR command (streaming all server activity).
    pub tx: broadcast::Sender<String>,
    /// MPSC channel for the AOF worker (logging write operations to disk).
    pub aof_tx: Arc<mpsc::Sender<String>>,
}

impl ShardedDb {
    /// Bootstraps a new database.
    pub fn new_db() -> (ShardedDb, mpsc::Receiver<String>) {
        let mut shards_vec = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards_vec.push(RwLock::new(HashMap::new()));
        }
        
        // Convert the Vec into a fixed-size array to keep it on the stack/heap efficiently.
        let shards_boxed: Box<[RwLock<HashMap<Bytes, Entry>>; SHARD_COUNT]> =
            shards_vec.into_boxed_slice().try_into().expect("Failed to create shards");

        let (tx, _rx) = broadcast::channel(1024);
        let (aof_tx, aof_rx) = mpsc::channel(100_000);

        let db = ShardedDb {
            shards: Arc::from(shards_boxed),
            tx,
            aof_tx: Arc::new(aof_tx),
        };

        (db, aof_rx)
    }

    /// Determines which shard a key belongs to using a stable hash.
    /// Bitwise AND (&) is used instead of modulo (%) for speed (requires power-of-2 shard count).
    fn calculate_shard_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_result = hasher.finish();
        (hash_result & SHARD_MASK) as usize
    }

    /// Acquires a Read lock on the shard containing the given key.
    pub async fn read_shard(&self, key: &[u8]) -> RwLockReadGuard<'_, HashMap<Bytes, Entry>> {
        let index = self.calculate_shard_index(key);
        self.shards[index].read().await
    }

    /// Acquires a Write lock on the shard containing the given key.
    pub async fn write_shard(&self, key: &[u8]) -> RwLockWriteGuard<'_, HashMap<Bytes, Entry>> {
        let index = self.calculate_shard_index(key);
        self.shards[index].write().await
    }

    /// Deadlock-safe multi-write locking.
    /// To safely lock two shards, we MUST always acquire them in the same order (e.g., lower index first).
    /// If we don't, two threads locking A->B and B->A will deadlock.
    pub async fn write_multi_shards<'a>(&'a self, key_a: &[u8], key_b: &[u8]) -> MultiWriteGuard<'a> {
        let idx_a = self.calculate_shard_index(key_a);
        let idx_b = self.calculate_shard_index(key_b);

        if idx_a == idx_b {
            return MultiWriteGuard::Single(self.shards[idx_a].write().await);
        }

        // Lock sorting: Always lock the smaller index first.
        if idx_a < idx_b {
            let guard_a = self.shards[idx_a].write().await;
            let guard_b = self.shards[idx_b].write().await;
            MultiWriteGuard::Double(guard_a, guard_b)
        } else {
            let guard_b = self.shards[idx_b].write().await;
            let guard_a = self.shards[idx_a].write().await;
            MultiWriteGuard::Double(guard_a, guard_b)
        }
    }

    /// Deadlock-safe multi-read locking using the same sorting pattern as write_multi_shards.
    pub async fn read_multi_shards<'a>(&'a self, key_a: &[u8], key_b: &[u8]) -> MultiReadGuard<'a> {
        let idx_a = self.calculate_shard_index(key_a);
        let idx_b = self.calculate_shard_index(key_b);

        if idx_a == idx_b {
            return MultiReadGuard::Single(self.shards[idx_a].read().await);
        }

        if idx_a < idx_b {
            let guard_a = self.shards[idx_a].read().await;
            let guard_b = self.shards[idx_b].read().await;
            MultiReadGuard::Double(guard_a, guard_b)
        } else {
            let guard_a = self.shards[idx_a].read().await;
            let guard_b = self.shards[idx_b].read().await;
            MultiReadGuard::Double(guard_a, guard_b)
        }
    }

    pub fn get_shard_count(&self) -> usize {
        SHARD_COUNT
    }

    pub async fn read_shard_by_index(
        &self,
        index: usize
    ) -> RwLockReadGuard<'_, HashMap<Bytes, Entry>> {
        self.shards[index].read().await
    }

    pub async fn write_shard_by_index(
        &self,
        index: usize
    ) -> RwLockWriteGuard<'_, HashMap<Bytes, Entry>> {
        self.shards[index].write().await
    }

    /// Aggregates all keys from all shards. This is an expensive O(N) operation.
    pub async fn get_all_keys(&self) -> Vec<Bytes> {
        let mut collected_keys = Vec::new();

        for i in 0..SHARD_COUNT {
            let shard = self.shards[i].read().await;
            for key in shard.keys() {
                collected_keys.push(key.clone()); // O(1) cloning (reference bump)
            }
        }

        collected_keys
    }

    /// Returns all keys within a specific shard. Used for the SCAN command.
    pub async fn scan_shard(&self, cursor: usize) -> Vec<Bytes> {
        if cursor >= SHARD_COUNT {
            return Vec::new();
        }
        let shard = self.shards[cursor].read().await;
        shard.keys().cloned().collect()
    }
}

impl Clone for ShardedDb {
    fn clone(&self) -> Self {
        Self {
            shards: Arc::clone(&self.shards),
            tx: self.tx.clone(),
            aof_tx: Arc::clone(&self.aof_tx),
        }
    }
}
