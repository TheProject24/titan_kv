use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type Db = Arc<RwLock<HashMap<String, String>>>;

pub fn new_db() -> Db {
    Arc::new(RwLock::new(HashMap::new()))
}