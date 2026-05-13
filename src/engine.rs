// src/engine.rs

use std::collections::{HashMap, VecDeque};
// use std::sync::{Arc, RwLock};
use std::sync::Arc;
use tokio::sync::{RwLock};
use std::time::SystemTime;

pub type Db = Arc<RwLock<HashMap<String, Entry>>>;

pub struct Entry {
    pub value: DataType,
    pub expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub enum DataType {
    String(String),
    List(VecDeque<String>),
    HashMap(HashMap<String, String>),
}

pub fn new_db() -> Db {
    Arc::new(RwLock::new(HashMap::new()))
}