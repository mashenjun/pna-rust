//! # KvStore Crate

pub use crate::{KvsError, Result};

use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::io::{Write, Seek, SeekFrom, Read};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use std::fs;


/// KvStore store data in memory
pub struct KvStore {
    path: PathBuf,
    // use to store the index position and length to key
    index: HashMap<String, Meta>,
    cursor: u64,
    // db: HashMap<String, String>,
}

#[allow(clippy::new_without_default)]
impl KvStore {
    /// open read a file with the given path
    pub fn open(path: impl Into<PathBuf>)-> Result<KvStore>{
        let path = path.into();
        fs::create_dir_all(&path)?;
        let db_path = path.join("db");
        let mut index :HashMap<String, Meta> = HashMap::new();
        let mut file = OpenOptions::new().read(true).create(true).write(true).open(&db_path)?;
        let mut cursor = file.seek(SeekFrom::Start(0))?;
        let decoder = serde_json::Deserializer::from_reader(file);
        let mut iterator = decoder.into_iter::<Command>();
        // TODO: can use better op than match
        while let Some(cmd) = iterator.next() {
            let new_cursor = iterator.byte_offset() as u64;
            match cmd? {
                Command::Set{key, ..} => {
                    index.insert(key, Meta(cursor, new_cursor as u64 - cursor));
                },
                Command::Remove{key} => {
                    index.remove(&key);
                },
                _ => (),
            }
            cursor = new_cursor as u64;
        }
        Ok(KvStore {
            // path : path.join("db"),
            path: db_path,
            index,
            cursor, // TODO: use real cursor value
        })
    }
    /// set write the given key value into log file and update the index map.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // TODO: prototype
        //       open the file defined by `path`, append string into file and close it;
        //       insert key value to in hash mapKeyNotFoundError
        //       no need to build a command again
        let cmd = Command::Set { key, value };
        let mut file = OpenOptions::new().append(true).create(true).open(&self.path)?;
        let vec = serde_json::to_vec(&cmd)?;
        let buf = vec.as_ref();
        file.write(buf)?;
        // update the cursor
        file.flush()?;
        if let Command::Set { key, ..} = cmd {
            self.index.insert(key, Meta(self.cursor as u64, buf.len() as u64));
        };
        self.cursor += buf.len() as u64;
        Ok(())
    }

    /// get use internal index to find the meta data and fetch kv from disk, if no key should return None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(meta) = self.index.get(&key) {
            // fetch kv form disk using the meta
            let mut file = OpenOptions::new().read(true).create(true).write(true).open(&self.path)?;
            file.seek(SeekFrom::Start(meta.0))?;
            let cmd_reader = file.take(meta.1);
            return if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::InvalidCommandError)
            }
        }
        Ok(None)
    }

    /// remove call internal HashMap remove api to remove data
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.index.remove(&key) {
            Some(_) => {
                let cmd = Command::Remove { key };
                let mut file = OpenOptions::new().append(true).create(true).open(&self.path)?;
                serde_json::to_writer(&mut file, &cmd)?;
                file.flush()?;
                Ok(())
            },
            None => Err(KvsError::KeyNotFoundError)
        }
    }
}

/// Command defines command
#[derive(StructOpt, Debug, Serialize, Deserialize)]
pub enum Command {
    #[structopt(name = "get")]
    Get {
        key: String,
    },
    #[structopt(name = "set")]
    Set {
        key: String,
        value: String,
    },
    #[structopt(name = "rm")]
    Remove {
        key: String,
    },
}

// Meta store position and length for a Set Command
#[derive(Debug)]
struct Meta(u64, u64); // position and length