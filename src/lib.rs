#[macro_use]
extern crate log;

pub use engines::KvsEngine;
pub use error::{KvsError, Result};
pub use kv_store::{Command, KvStore};
pub use proto::Request;
pub use server::KvsServer;

mod client;
mod engines;
mod error;
mod kv_store;
mod proto;
mod server;
