use crate::{KvsEngine, KvsError, Result};
use std::path::PathBuf;
use std::{fs, str};

#[derive(Clone)]
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let db: sled::Db = sled::open(path.join("sled_data"))?;
        Ok(SledKvsEngine { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        // flush in every set opt will make the opt too slow.
        // self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.db.get(key)?;
        let ret = match value {
            Some(data) => {
                let s = str::from_utf8(data.as_ref())?;
                Ok(Some(s.to_string()))
            }
            None => Ok(None),
        };
        ret
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFoundError)?;
        // flush in every remove opt will make the opt too slow.
        // self.db.flush()?;
        Ok(())
    }
}

// It may not be the best place to flush memory to disk.
impl Drop for SledKvsEngine {
    fn drop(&mut self) {
        if let Err(e) = self.db.flush() {
            eprintln!("{:?}", e);
        }
    }
}
