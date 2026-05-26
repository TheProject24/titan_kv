// src/engine.rs


pub type Db = ShardedDb;

pub fn new_db() -> Db {
    ShardedDb::new()
}

pub enum MultiWriteGuard<'a> {
    Single(RwLockWriteGuard<'a, HashMap<String, Entry>>),
    Double(
        RwLockWriteGuard<'a, HashMap<String, Entry>>,
        RwLockWriteGuard<'a, HashMap<String, Entry>>
    )
}

pub enum MultiReadGuard<'a> {
    Single(RwLockReadGuard<'a, HashMap<String, Entry>>),
    Double(
        RwLockReadGuard<'a, HashMap<String, Entry>>,
        RwLockReadGuard<'a, HashMap<String, Entry>>
    )  
}

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
use std::time::SystemTime;

const SHARD_COUNT: usize = 64;
const SHARD_MASK: u64 = (SHARD_COUNT - 1) as u64;

#[derive(Clone)]
pub enum DataType {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
    Set(HashSet<String>),
}

pub struct Entry {
    pub value: DataType,
    pub expires_at: Option<SystemTime>,
}

pub struct ShardedDb {
    shards: Arc<[RwLock<HashMap<String, Entry>>; SHARD_COUNT]>,
}

impl ShardedDb {
    pub fn new() -> Self {
        let mut shards_vec = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards_vec.push(RwLock::new(HashMap::new()));
        }

        let shards_boxed: Box<[RwLock<HashMap<String, Entry>>; SHARD_COUNT]> = 
            shards_vec.into_boxed_slice().try_into().unwrap_or_else(|_| {
                panic!("Failed to initialize database shard array structure.")
            });

        Self {
            shards: Arc::from(shards_boxed),
        }
    }

    fn calculate_shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_result = hasher.finish();
        (hash_result & SHARD_MASK) as usize
    }

    pub async fn read_shard(&self, key: &str) -> RwLockReadGuard<'_, HashMap<String, Entry>> {
        let index = self.calculate_shard_index(key);
        self.shards[index].read().await
    }

    pub async fn write_shard(&self, key: &str) -> RwLockWriteGuard<'_, HashMap<String, Entry>> {
        let index = self.calculate_shard_index(key);
        self.shards[index].write().await
    }

    pub async fn write_multi_shards<'a>(&'a self, key_a: &str, key_b: &str) -> MultiWriteGuard<'a> {
        let idx_a = self.calculate_shard_index(key_a);
        let idx_b = self.calculate_shard_index(key_b);

        if idx_a == idx_b {
            return MultiWriteGuard::Single(self.shards[idx_a].write().await);
        }

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

    pub async fn read_multi_shards<'a>(&'a self, key_a: &str, key_b: &str) -> MultiReadGuard<'a> {
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

    pub async fn read_shard_by_index(&self, index: usize) -> RwLockReadGuard<'_, HashMap<String, Entry>> {
        self.shards[index].read().await
    }

    pub async fn write_shard_by_index(&self, index: usize) -> RwLockWriteGuard<'_, HashMap<String, Entry>> {
        self.shards[index].write().await
    }

    pub async fn get_all_keys(&self) -> Vec<String> {
        let mut collected_keys = Vec::new();

        for i in 0..SHARD_COUNT {
            let shard = self.shards[i].read().await;

            for key in shard.keys() {
                collected_keys.push(key.clone());
            }
        }

        collected_keys
    }

    pub async fn scan_shard(&self, cursor: usize) -> Vec<String> {
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
        }
    }
}