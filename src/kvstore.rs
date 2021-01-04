//! # KvStore Crate

pub use crate::error::{KvsError, Result};

use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::io::Write;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use std::fs;

/// KvStore store data in memory
pub struct KvStore {
    path: PathBuf,
    // pathBuf: PathBuf,
    db: HashMap<String, String>,
}

#[allow(clippy::new_without_default)]
impl KvStore {
    /// new init a instance of KvStore by using HashMap
    // pub fn new(path: &Path) -> Self {
    //     Self { path: *path.clone(), db: HashMap::new() }
    // }

    /// open read a file with the given path
    pub fn open(path: impl Into<PathBuf>)-> Result<KvStore>{
        let path = path.into();
        fs::create_dir_all(&path)?;
        let db_path = path.join("db");
        let mut data :HashMap<String, String> = HashMap::new();
        let file = OpenOptions::new().read(true).create(true).write(true).open(&db_path)?;
        let decoder = serde_json::Deserializer::from_reader(file);
        let iterator = decoder.into_iter::<Command>();
        // TODO: can use better op than match
        for item in iterator {
            match item {
                Ok(cmd) => {
                    match cmd {
                        Command::Set{key:k, value: v} => {
                            data.insert(k, v);
                        },
                        Command::Remove{key:k} => {
                            data.remove(&k);
                        },
                        _ =>(),
                    }
                },
                _ => (),
            }
        }
        Ok(KvStore {
            // path : path.join("db"),
            path: db_path,
            db: data // an in memory hash map
        })
    }

    /// set use internal HashMap to set data
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // TODO:prototype
        //      open the file defined by `path`, append string into file and close it;
        //      insert key value to in hash map
        //      no need to build a command again
        let cmd = Command::Set { key, value };
        let mut file = OpenOptions::new().append(true).create(true).open(&self.path)?;
        serde_json::to_writer(&mut file, &cmd)?;
        file.flush()?;
        if let Command::Set {key, value} = cmd {
            self.db.insert(key, value);
        };
        Ok(())
    }

    /// get use internal HashMap to get data
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // let mut file = OpenOptions::new().read(true).create(true).open(&self.path)?;
        self.db.get(&key).cloned().map_or(Ok(None), |v| Ok(Some(v)))
    }

    /// remove call internal HashMap remove api to remove data
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(&key).map(|_| ()).ok_or(KvsError::MockError)
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
