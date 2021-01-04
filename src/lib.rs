pub use kvstore::{KvStore,Command};
pub use error::{KvsError, Result};

mod kvstore;
mod error;