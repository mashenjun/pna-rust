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

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
