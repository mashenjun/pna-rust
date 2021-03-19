//! # KvStore Crate

pub use crate::{KvsError, Result};

use crate::KvsEngine;
use positioned_io::ReadAt;
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Mutex};
use crossbeam_skiplist::SkipMap;
use crossbeam::atomic::AtomicCell;
use std::sync::atomic::*;
use std::cell::RefCell;
use crate::error::KvsError::IOError;


const COMPACT_THRESHOLD_BYTES: u64 = 1024 * 1024;

/// KvStore store data in memory, without read write lock-free
#[derive(Clone)]
pub struct KvOldStore {
    path: Arc<PathBuf>,
    // index stores key to the position in file and length.
    // TODO: a simple RwLock doesn't make it lock free.
    db: Arc<RwLock<KvDB>>,
}

struct KvDB {
    // what we need to protect is the index along with the internal meta data.
    // for read and write.
    index: HashMap<String, Meta>,
    // for reading, since we use pread here, we not need to track cursor for reader, we don't even need to protect reader.
    // what we what to achieve is using some lock-free struct to save index. Thus reader is allowed to access the index without any locking.
    // considering use the crossbeam::skip_list::SkipMap here.
    reader: File,
    // for write.
    cursor: u64,
    dangling_bytes: u64,
    writer: File,   // for writing
}

impl KvOldStore {
    /// open read a file with the given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let mut index: HashMap<String, Meta> = HashMap::new();
        let mut reader = open_for_read(&data_path(&path), 0)?;
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
        Ok(Self {
            path: Arc::new(path),
            db: Arc::new(RwLock::new(KvDB {
                index,
                cursor,
                dangling_bytes,
                reader,
                writer: file,
            })),
        })
    }
}

impl KvDB {
    fn compact(&mut self, path: &Path) -> Result<()> {
        // nothing can do if threshold not match.
        if self.dangling_bytes <= COMPACT_THRESHOLD_BYTES {
            return Ok(());
        }
        // do real compaction
        let compact_path = compact_path(path);
        let mut compact_file: File = open_for_append(&compact_path)?;
        compact_file.seek(SeekFrom::Start(0))?;
        let mut cursor = 0;
        for meta in self.index.values_mut() {
            let mut buf = vec![0u8; meta.1 as usize];
            self.reader.read_exact_at(meta.0, buf.as_mut())?;
            let l = io::copy(&mut &buf[..], compact_file.borrow_mut())?;
            *meta = Meta(cursor, l);
            cursor += l;
        }
        compact_file.flush()?;
        // update file and reader
        // todo: this swap need lock which break the lock free.
        self.writer = compact_file;
        self.reader = open_for_read(&compact_path, 0)?;
        let data_path = data_path(path);
        fs::remove_file(&data_path)?;
        fs::rename(compact_path, data_path)?;
        self.dangling_bytes = 0;
        Ok(())
    }
}

#[allow(clippy::new_without_default)]
impl KvsEngine for KvOldStore {
    /// `set` append the given key value into log file and update the index map.
    /// the content will be flush to disk immediately.
    fn set(&self, key: String, value: String) -> Result<()> {
        // todo: the RwLock is not the solution to lock-free
        // consider evmap??
        let mut db = self.db.write().unwrap();
        let cmd = Command::Set { key, value };
        let vec = serde_json::to_vec(&cmd)?;
        let buf = vec.as_ref();
        db.writer.write_all(buf)?;
        // update the cursor
        db.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            let cursor = db.cursor;
            if let Some(meta) = db.index.insert(key, Meta::new(cursor, buf.len() as u64)) {
                db.dangling_bytes += meta.1;
            }
        };
        db.cursor += buf.len() as u64;
        db.compact(self.path.as_path())?;
        Ok(())
    }

    /// `get` use internal index to find the meta data and fetch kv from disk, if no key should return None
    fn get(&self, key: String) -> Result<Option<String>> {
        let db = self.db.read().unwrap();
        if let Some(meta) = db.index.get(&key) {
            // fetch kv form disk using the meta
            let mut buf = vec![0u8; meta.1 as usize];
            // the read_exact_at call pread under the hood.
            db.reader.read_exact_at(meta.0, buf.as_mut())?;
            return if let Command::Set { value, .. } = serde_json::from_slice(buf.as_ref())?
            {
                Ok(Some(value))
            } else {
                Err(KvsError::InvalidCommandError)
            };
        }
        Ok(None)
    }

    /// `remove` call internal HashMap remove api to remove data
    fn remove(&self, key: String) -> Result<()> {
        let mut db = self.db.write().unwrap();
        match db.index.remove(&key) {
            Some(meta) => {
                let cmd = Command::Remove { key };
                serde_json::to_writer(&mut db.writer, &cmd)?;
                db.writer.flush()?;
                db.dangling_bytes += meta.1;
                Ok(())
            }
            None => Err(KvsError::KeyNotFoundError),
        }
    }
}

#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    reader: Arc<LeftRight>, // since LeftRight impl Send and !Sync, we can not use Arc<LeftRight>, Arc<LeftRight> impl !Send and !Sync
    writer: Arc<Mutex<IndexWriter>>,
}

impl KvStore {
    /// open read a file with the given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        let index: SkipMap<String, Meta> = SkipMap::new();
        let mut reader = open_for_read(&data_path(&path), 0)?;
        let decoder = serde_json::Deserializer::from_reader(&mut reader);
        let mut iterator = decoder.into_iter::<Command>();
        let mut dangling_bytes: u64 = 0;
        // TODO: can use better op than match
        let mut cursor: u64 = 0;
        while let Some(cmd) = iterator.next() {
            let new_cursor = iterator.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    let entry = index.insert(key, Meta(cursor, new_cursor as u64 - cursor));
                    let meta = entry.value();
                    dangling_bytes += meta.1;
                }
                Command::Remove { key } => {
                    index.remove(&key);
                }
                _ => (),
            }
            cursor = new_cursor as u64;
        }

        let arc_index = Arc::new(index);
        let left = IndexReader::new(path.clone(),open_for_read(&data_path(&path), 0)?, arc_index.clone());
        let right = IndexReader::new(path.clone(),open_for_read(&data_path(&path), 0)?, arc_index.clone());
        let left_right_reader = LeftRight{
            cnt: Arc::new(AtomicU32::new(0)),
            left: left,
            right: right,
        };
        let arc_left_right_reader = Arc::new(left_right_reader);
        let index_writer = IndexWriter{
            dir: path.clone(),
            left_right_reader: arc_left_right_reader.clone(),
            index : arc_index,
            cursor,
            dangling_bytes,
            writer: open_for_append(&data_path(&path))?,
        };
        Ok(Self {
            path: Arc::new(path),
            reader: arc_left_right_reader,
            writer: Arc::new(Mutex::new(index_writer)),
            })
        }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.reader.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.remove(key)
    }
}

// since the SkipMap is a lock-free struct, and we use pread to access the fd underline. No lock is need here.
// if we use RefCell on reader then impl IndexReader impl Send and !Sync
struct IndexReader {
    dir: PathBuf,
    reader: AtomicCell<File>,
    // we may need to replace the map in compact.
    index : Arc<SkipMap<String, Meta>>,
}

impl Clone for IndexReader {
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            reader: AtomicCell::new(open_for_read(self.dir.as_path(), 0).expect("data file broken")),
            index: self.index.clone(),
        }
    }
}

impl IndexReader {
    pub fn new(dir: PathBuf, reader: File, index: Arc<SkipMap<String, Meta>>) -> Self {
        return IndexReader{
            dir,
            reader: AtomicCell::new( reader),
            index
        }
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.index.get(&key) {
            let meta = entry.value();
            // fetch kv form disk using the meta
            let mut buf = vec![0u8; meta.1 as usize];
            // the read_exact_at call pread under the hood.
            // TODO: what is the safety here?
            let reader = unsafe {
                self.reader.as_ptr().as_ref().ok_or(io::Error::new(io::ErrorKind::NotFound, "broken file in atomic cell"))
            };
            let reader = reader?;
            reader.read_exact_at(meta.0, buf.as_mut())?;
            return if let Command::Set { value, .. } = serde_json::from_slice(buf.as_ref())?
            {
                Ok(Some(value))
            } else {
                Err(KvsError::InvalidCommandError)
            };
        }
        Ok(None)
    }

    pub fn reopen(&self)->Result<()> {
        let reader = open_for_read(data_path( self.dir.as_path()).as_path(), 0)?;
        // calling store will drop the old reader.
        self.reader.store(reader);
        Ok(())
    }

    pub fn get_index(&self) -> Arc<SkipMap<String, Meta>> {
        self.index.clone()
    }
}

struct LeftRight {
    cnt: Arc<AtomicU32>, // Atomic impl Send and Sync
    left: IndexReader, // RefCall impl Send and !Sync
    right: IndexReader, // RefCall impl Send and !Sync
}

impl Clone for LeftRight {
    fn clone(&self) -> Self {
        Self {
            cnt: self.cnt.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl LeftRight{
    fn get(&self, key: String) -> Result<Option<String>> {
        return match self.cnt.load(Ordering::Acquire) {
            0 => {
                self.left.get(key)
            }
            1 => {
                self.right.get(key)
            }
            _ => {
                unreachable!()
            }
        }
    }

    fn compact_index(&self) -> Arc<SkipMap<String, Meta>> {
        return match self.cnt.load(Ordering::Acquire) {
            0 => {
                self.right.get_index()
            },
            1 => {
                self.left.get_index()
            }
            _ => {
                unreachable!()
            }
        }
    }
    // `compact_reopen` must call after the compact_index updated
    fn compact_reopen(&self) -> Result<()>  {
       match self.cnt.load(Ordering::Acquire) {
           0 => {
               self.right.reopen()?;
               self.cnt.store(1, Ordering::Release);
           },
           1 => {
               self.left.reopen()?;
               self.cnt.store(0, Ordering::Release);
           }
           _ => {
               unreachable!()
           }
       }
        Ok(())
    }
}

struct IndexWriter{
    // we may mut the index_reader
    dir: PathBuf,
    left_right_reader: Arc<LeftRight>, // use Arc<LeftRight> here
    index : Arc<SkipMap<String, Meta>>,
    cursor: u64,
    dangling_bytes: u64,
    writer: File
}

impl IndexWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };
        let vec = serde_json::to_vec(&cmd)?;
        let buf = vec.as_ref();
        self.writer.write_all(buf)?;
        // update the cursor
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            let cursor = self.cursor;
            let entry =  self.index.insert(key, Meta::new(cursor, buf.len() as u64));
            let meta = entry.value();
            self.dangling_bytes += meta.1;
        };
        self.cursor += buf.len() as u64;
        let path = self.dir.clone();
        self.compact(path.as_path())?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.index.remove(&key) {
            Some(entry) => {
                let meta = entry.value();
                let cmd = Command::Remove { key };
                serde_json::to_writer(&mut self.writer, &cmd)?;
                self.writer.flush()?;
                self.dangling_bytes += meta.1;
                Ok(())
            }
            None => Err(KvsError::KeyNotFoundError),
        }
    }

    // the hard part is how to update IndexReader???
    fn compact(&mut self, dir: &Path) -> Result<()> {
        // nothing can do if dangling_bytes not excess the threshold.
        if self.dangling_bytes <= COMPACT_THRESHOLD_BYTES {
            return Ok(());
        }
        // do real compaction
        let compact_to_path = compact_path(dir);
        let compact_from_path = data_path(dir);
        let mut compact_file: File = open_for_append(&compact_to_path)?;
        compact_file.seek(SeekFrom::Start(0))?;
        let mut cursor = 0;
        // do the in place modify
        {
            let compact_index = self.left_right_reader.compact_index();
            let data_file = open_for_read(&compact_from_path, 0)?;
            for meta in self.index.iter() {
                let length = meta.value().1 as usize;
                let mut buf = vec![0u8; length];
                data_file.read_exact_at(meta.value().0, buf.as_mut())?;
                let l = io::copy(&mut &buf[..], compact_file.borrow_mut())?;
                compact_index.insert(meta.key().to_string(), Meta(cursor, l));
                cursor += l;
            }
        }
        compact_file.flush()?;

        // update index_writer.
        self.writer = compact_file;
        self.dangling_bytes = 0;

        // epilogue for clear
        let data_path = data_path(dir);
        fs::remove_file(&data_path)?;
        fs::rename(compact_to_path, data_path)?;
        // reopen the left_right_reader.
        self.left_right_reader.compact_reopen()?;
        Ok(())
    }
}

// data_path is the path to the current data file
fn data_path(path: &Path) -> PathBuf {
    path.join("data")
}

// compact_path is the path to the compact target file
fn compact_path(path: &Path) -> PathBuf {
    path.join("data.compact")
}

fn open_for_read(path: &Path, pos: u64) -> Result<File> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    file.seek(SeekFrom::Start(pos))?;
    Ok(file)
}

fn open_for_append(path: &Path) -> Result<File> {
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