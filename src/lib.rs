#[macro_use]
extern crate log;
extern crate nom;

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use proto::{parse_reply, parse_request, Reply, Request};
pub use server::KvsServer;

mod client;
mod engines;
mod error;
mod proto;
mod server;
