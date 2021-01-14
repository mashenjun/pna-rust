//! # KvStore Crate

pub use crate::{KvsError, Result};

use crate::KvsEngine;
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use structopt::StructOpt;

const COMPACT_THRESHOLD_BYTES: u64 = 1024 * 1024;

/// KvStore store data in memory
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    // index stores key to the position in file and length.
    db: Arc<RwLock<KvDB>>,
}

struct KvDB {
    index: HashMap<String, Meta>,
    cursor: u64,
    dangling_bytes: u64,
    reader: Mutex<BufReader<File>>, // TODO for reading
    file: File,                     // for writing
}

impl KvStore {
    /// open read a file with the given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let mut index: HashMap<String, Meta> = HashMap::new();
        let mut reader = BufReader::new(open_for_read(&data_path(&path), 0)?);
        let decoder = serde_json::Deserializer::from_reader(&mut reader);
        let mut iterator = decoder.into_iter::<Command>();
        let mut dangling_bytes: u64 = 0;
        // TODO: can use better op than match
        let mut cursor: u64 = 0;
        while let Some(cmd) = iterator.next() {
            let new_cursor = iterator.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    if let Some(meta) = index.insert(key, Meta(cursor, new_cursor as u64 - cursor))
                    {
                        dangling_bytes += meta.1
                    }
                }
                Command::Remove { key } => {
                    index.remove(&key);
                }
                _ => (),
            }
            cursor = new_cursor as u64;
        }
        let file = open_for_append(&data_path(&path))?;
        Ok(KvStore {
            path: Arc::new(path),
            db: Arc::new(RwLock::new(KvDB {
                index,
                cursor,
                dangling_bytes,
                reader: Mutex::new(reader),
                file,
            })),
        })
    }
}

impl KvDB {
    fn compact(&mut self, path: &PathBuf) -> Result<()> {
        // nothing can do on a PoisonError
        if self.dangling_bytes <= COMPACT_THRESHOLD_BYTES {
            return Ok(());
        }
        // do real compaction
        let mut reader = self.reader.lock().unwrap();
        let compact_path = compact_path(path);
        let mut compact_file: File = open_for_append(&compact_path)?;
        compact_file.seek(SeekFrom::Start(0))?;
        let mut cursor = 0;
        for meta in self.index.values_mut() {
            reader.seek(SeekFrom::Start(meta.0))?;
            let mut cmd_reader = reader.get_mut().take(meta.1);
            let l = io::copy(&mut cmd_reader, compact_file.borrow_mut())?;
            *meta = Meta(cursor, l);
            cursor += l;
        }
        compact_file.flush()?;
        // update file and reader
        self.file = compact_file;
        *reader = BufReader::new(open_for_read(&compact_path, 0)?);
        let data_path = data_path(path);
        fs::remove_file(&data_path)?;
        fs::rename(compact_path, data_path)?;
        self.dangling_bytes = 0;
        Ok(())
    }
}

#[allow(clippy::new_without_default)]
impl KvsEngine for KvStore {
    /// set write the given key value into log file and update the index map.
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut db = self.db.write().unwrap();
        // TODO: prototype
        //       open the file defined by `path`, append string into file and close it;
        //       insert key value to in hash mapKeyNotFoundError
        //       no need to build a command again
        let cmd = Command::Set { key, value };
        let vec = serde_json::to_vec(&cmd)?;
        let buf = vec.as_ref();
        db.file.write(buf)?;
        // update the cursor
        db.file.flush()?;
        if let Command::Set { key, .. } = cmd {
            let cursor = db.cursor; // TODO,
            if let Some(meta) = db.index.insert(key, Meta::new(cursor, buf.len() as u64)) {
                db.dangling_bytes += meta.1;
            }
        };
        db.cursor += buf.len() as u64;
        db.compact(self.path.as_ref())?;
        Ok(())
    }

    /// get use internal index to find the meta data and fetch kv from disk, if no key should return None
    fn get(&self, key: String) -> Result<Option<String>> {
        let db = self.db.read().unwrap();
        if let Some(meta) = db.index.get(&key) {
            // fetch kv form disk using the meta
            let mut reader = db.reader.lock().unwrap();
            reader.seek(SeekFrom::Start(meta.0))?;
            return if let Command::Set { value, .. } =
                serde_json::from_reader(reader.get_mut().take(meta.1))?
            {
                Ok(Some(value))
            } else {
                Err(KvsError::InvalidCommandError)
            };
        }
        Ok(None)
    }

    /// remove call internal HashMap remove api to remove data
    fn remove(&self, key: String) -> Result<()> {
        let mut db = self.db.write().unwrap();
        match db.index.remove(&key) {
            Some(meta) => {
                let cmd = Command::Remove { key };
                serde_json::to_writer(&mut db.file, &cmd)?;
                db.file.flush()?;
                db.dangling_bytes += meta.1;
                Ok(())
            }
            None => Err(KvsError::KeyNotFoundError),
        }
    }
}

fn data_path(path: &PathBuf) -> PathBuf {
    path.join("data")
}

fn compact_path(path: &PathBuf) -> PathBuf {
    path.join("data.compact")
}

fn open_for_read(path: &PathBuf, pos: u64) -> Result<File> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    file.seek(SeekFrom::Start(pos))?;
    Ok(file)
}

fn open_for_append(path: &PathBuf) -> Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(path)?;
    Ok(file)
}

/// Command defines command
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

impl Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Get { key } => {
                write!(f, "get {}", key)?;
            }
            Command::Set { key, value } => {
                write!(f, "set {}:{}", key, value)?;
            }
            Command::Remove { key } => {
                write!(f, "rm {}", key)?;
            }
        }
        Ok(())
    }
}

// Meta store position and length for a Set Command
#[derive(Debug)]
struct Meta(u64, u64); // position and length

impl Meta {
    pub fn new(p: u64, l: u64) -> Self {
        Meta(p, l)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::current_dir;
    #[test]
    fn open_for_read() {
        let path = current_dir().unwrap();
        super::open_for_read(&data_path(&path), 0).unwrap();
    }
}
