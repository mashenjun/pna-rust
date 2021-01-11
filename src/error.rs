use std::fmt::{self, Display};
use std::io;

/// My Error Type
#[derive(Debug)]
pub enum KvsError {
    /// KeyNotFoundError is for test usage
    KeyNotFoundError,
    InvalidCommandError,
    IOError(io::Error),
    SerdeJsonError(serde_json::Error),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IOError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::SerdeJsonError(err)
    }
}

impl Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvsError::IOError(e) => {
                write!(f, "Io error: {}", e)
            }
            KvsError::InvalidCommandError => {
                write!(f, "Invalid command")
            }
            KvsError::KeyNotFoundError => {
                write!(f, "Key not found")
            }
            KvsError::SerdeJsonError(e) => {
                write!(f, "Serde json error: {}", e)
            }
        }
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
