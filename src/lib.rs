//! # KvStore Crate
#![deny(missing_docs)]
use std::collections::HashMap;

/// KvStore store data in memory
pub struct KvStore {
    db: HashMap<String, String>,
}

#[allow(clippy::new_without_default)]
impl KvStore {
    /// new init a instance of KvStore by using HashMap
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// set use internal HashMap to set data
    pub fn set(&mut self, key: String, value: String) {
        self.db.insert(key, value);
    }

    /// get use internal HashMap to get data
    pub fn get(&mut self, key: String) -> Option<String> {
        self.db.get(&key).cloned()
    }

    /// remove call internal HashMap remove api to remove data
    pub fn remove(&mut self, key: String) {
        self.db.remove(&key);
    }
}
