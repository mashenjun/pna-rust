use crate::{KvsEngine, Result};

pub struct KvStorage {}

impl KvStorage {
    pub fn new() -> Self {
        KvStorage {}
    }
}

impl KvsEngine for KvStorage {
    fn set(&mut self, _: String, _: String) -> Result<()> {
        Ok(())
    }

    fn get(&mut self, _: String) -> Result<Option<String>> {
        Ok(None)
    }

    fn remove(&mut self, _: String) -> Result<()> {
        Ok(())
    }
}
