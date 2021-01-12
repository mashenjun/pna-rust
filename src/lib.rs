#[macro_use]
extern crate log;

pub use client::KvsClient;
pub use engines::{KvStorage, KvsEngine};
pub use error::{KvsError, Result};
pub use kv_store::{Command, KvStore};
pub use server::KvsServer;

mod client;
mod engines;
mod error;
mod kv_store;
mod proto;
mod server;
