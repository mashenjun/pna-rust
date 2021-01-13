use crate::{KvsEngine, KvsError, Result};
use std::path::PathBuf;
use std::{fs, str};

pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let db: sled::Db = sled::open(path.join("sled_data"))?;
        Ok(SledKvsEngine { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
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

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFoundError)?;
        self.db.flush()?;
        Ok(())
    }
}
