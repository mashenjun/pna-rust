//! # KvStore Crate

pub use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use structopt::StructOpt;

const COMPACT_THRESHOLD_BYTES: u64 = 1024 * 1024;

/// KvStore store data in memory
pub struct KvStore {
    path: PathBuf,
    // index stores key to the position in file and length.
    index: HashMap<String, Meta>,
    cursor: u64,
    dangling_bytes: u64,
    file: File,
}

#[allow(clippy::new_without_default)]
impl KvStore {
    /// open read a file with the given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let mut index: HashMap<String, Meta> = HashMap::new();
        let mut file: File = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&data_path(&path))?;
        let mut cursor: u64 = file.seek(SeekFrom::Start(0))?;
        let decoder = serde_json::Deserializer::from_reader(&mut file);
        let mut iterator = decoder.into_iter::<Command>();
        let mut dangling_bytes: u64 = 0;
        // TODO: can use better op than match
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
        Ok(KvStore {
            path,
            index,
            cursor,
            dangling_bytes,
            file,
        })
    }
    /// set write the given key value into log file and update the index map.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // TODO: prototype
        //       open the file defined by `path`, append string into file and close it;
        //       insert key value to in hash mapKeyNotFoundError
        //       no need to build a command again
        let cmd = Command::Set { key, value };
        // let mut file = OpenOptions::new()
        //     .append(true)
        //     .create(true)
        //     .open(&self.path)?;
        let vec = serde_json::to_vec(&cmd)?;
        let buf = vec.as_ref();
        self.file.write(buf)?;
        // update the cursor
        self.file.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(meta) = self
                .index
                .insert(key, Meta(self.cursor as u64, buf.len() as u64))
            {
                self.dangling_bytes += meta.1;
            }
        };
        self.cursor += buf.len() as u64;
        self.compact()?;
        Ok(())
    }

    /// get use internal index to find the meta data and fetch kv from disk, if no key should return None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(meta) = self.index.get(&key) {
            // fetch kv form disk using the meta
            self.file.seek(SeekFrom::Start(meta.0))?;
            let cmd_reader = self.file.borrow_mut().take(meta.1);
            return if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::InvalidCommandError)
            };
        }
        Ok(None)
    }

    /// remove call internal HashMap remove api to remove data
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.index.remove(&key) {
            Some(meta) => {
                let cmd = Command::Remove { key };
                // let mut file = OpenOptions::new()
                //     .append(true)
                //     .create(true)
                //     .open(&self.path)?;
                serde_json::to_writer(&mut self.file, &cmd)?;
                self.file.flush()?;
                self.dangling_bytes += meta.1;
                Ok(())
            }
            None => Err(KvsError::KeyNotFoundError),
        }
    }

    fn compact(&mut self) -> Result<()> {
        if self.dangling_bytes <= COMPACT_THRESHOLD_BYTES {
            return Ok(());
        }
        println!("start compact with bytes {}", self.dangling_bytes);
        // do real compaction
        let compact_path = compact_path(&self.path);
        let mut compact_file: File = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&compact_path)?;

        compact_file.seek(SeekFrom::Start(0))?;
        let mut cursor = 0;
        for meta in self.index.values_mut() {
            self.file.seek(SeekFrom::Start(meta.0))?;
            let mut cmd_reader = self.file.borrow_mut().take(meta.1);
            let l = io::copy(&mut cmd_reader, compact_file.borrow_mut())?;
            *meta = Meta(cursor, l);
            cursor += l;
        }
        compact_file.flush()?;
        self.file = compact_file;
        let data_path = data_path(&self.path);
        fs::remove_file(&data_path)?;
        fs::rename(compact_path, data_path)?;
        self.dangling_bytes = 0;
        Ok(())
    }
}

fn data_path(path: &PathBuf) -> PathBuf {
    path.join("data")
}

fn compact_path(path: &PathBuf) -> PathBuf {
    path.join("data.compact")
}

/// Command defines command
#[derive(StructOpt, Debug, Serialize, Deserialize)]
pub enum Command {
    #[structopt(name = "get")]
    Get { key: String },
    #[structopt(name = "set")]
    Set { key: String, value: String },
    #[structopt(name = "rm")]
    Remove { key: String },
}

// Meta store position and length for a Set Command
#[derive(Debug)]
struct Meta(u64, u64); // position and length
