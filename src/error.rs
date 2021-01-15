use rayon::ThreadPoolBuildError;
use std::fmt::{self, Display};
use std::io;
use std::str::Utf8Error;

/// My Error Type
#[derive(Debug)]
pub enum KvsError {
    /// KeyNotFoundError is for test usage
    KeyNotFoundError,
    InvalidCommandError,
    IOError(io::Error),
    SerdeJsonError(serde_json::Error),
    Utf8Error(Utf8Error),
    SledError(sled::Error),
    RayonError(rayon::ThreadPoolBuildError),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::IOError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::SerdeJsonError(err)
    }
}

impl From<Utf8Error> for KvsError {
    fn from(err: Utf8Error) -> Self {
        KvsError::Utf8Error(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::SledError(err)
    }
}

impl From<rayon::ThreadPoolBuildError> for KvsError {
    fn from(err: ThreadPoolBuildError) -> Self {
        KvsError::RayonError(err)
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
            KvsError::Utf8Error(e) => {
                write!(f, "Utf8 error: {}", e)
            }
            KvsError::SledError(e) => {
                write!(f, "Sled error: {}", e)
            }
            KvsError::RayonError(e) => {
                write!(f, "Rayon error: {}", e)
            }
        }
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
