use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type Db = Arc<Mutex<HashMap<String, String>>>;

pub fn new_db() -> Db {
    Arc::new(Mutex::new(HashMap::new()))
}