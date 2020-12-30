//! # KvStore Crate
#![deny(missing_docs)]

mod error;

use std::collections::HashMap;
pub use error::{KvsError, Result};
use std::path::{Path};

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

    /// open read a file with the given path
    pub fn open(_path: &Path)-> Result<KvStore>{
        unimplemented!()
    }

    /// set use internal HashMap to set data
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // todo
        self.db.insert(key, value).map(|_| ()).ok_or(KvsError::MockError)
    }

    /// get use internal HashMap to get data
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // todo
        self.db.get(&key).cloned().map(|v| Some(v)).ok_or(KvsError::MockError)
    }

    /// remove call internal HashMap remove api to remove data
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(&key).map(|_| ()).ok_or(KvsError::MockError)
    }
}
