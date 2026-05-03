use std::collections::HashMap;
// use std::sync::{Arc, RwLock};
use std::sync::Arc;
use tokio::sync::{RwLock};
use std::time::SystemTime;

pub type Db = Arc<RwLock<HashMap<String, Entry>>>;

pub struct Entry {
    pub value: String,
    pub expires_at: Option<SystemTime>,
}

pub fn new_db() -> Db {
    Arc::new(RwLock::new(HashMap::new()))
}