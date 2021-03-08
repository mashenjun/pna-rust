// a very simple RESP serde Serializer and Deserializer trait
#![allow(dead_code, unused_must_use, unused_variables)]
#![allow(unused_imports)]
mod de;
mod error;
mod ser;

pub use de::{from_buf_reader, from_str, SimpleDeserializer};
pub use error::{Error, Result};
pub use ser::{to_string, to_writer, SimpleSerializer};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
// internal use
enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

impl Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Get { key } => {
                write!(f, "get {}", key)?;
            }
            Request::Set { key, value } => {
                write!(f, "set {}:{}", key, value)?;
            }
            Request::Remove { key } => {
                write!(f, "remove {}", key)?;
            }
        }
        Ok(())
    }
}

impl Request {
    // simply format using REdis Serialization Protocol
    pub fn to_resp(&self) -> String {
        let s = match self {
            Request::Get { key } => format!("*2\r\n{}\r\n{}\r\n", "GET", key),
            Request::Set { key, value } => {
                format!("*3\r\n{}\r\n{}\r\n{}\r\n", "SET", key, value)
            }
            Request::Remove { key } => format!("*2\r\n{}\r\n{}\r\n", "DEL", key),
        };
        s
    }
}

// TODO: impl Serialize and Deserialize for Reply
#[derive(Debug, Serialize, Deserialize, PartialEq)]
// internal use
enum Reply {
    SingleLine(String),
    Err(String),
    Int(i64),
}

impl Display for Reply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reply::SingleLine(s) => {
                write!(f, "{}", s)?;
            }
            Reply::Err(s) => {
                write!(f, "{}", s)?;
            }
            Reply::Int(s) => {
                write!(f, "{}", s)?;
            }
        }
        Ok(())
    }
}

impl Reply {
    pub fn to_resp(&self) -> String {
        let s = match self {
            Reply::SingleLine(data) => format!("+{}\r\n", data),
            Reply::Err(data) => format!("-{}\r\n", data),
            Reply::Int(data) => format!(":{}\r\n", data),
        };
        s
    }
    pub fn should_println(&self) -> bool {
        match self {
            Reply::SingleLine(data) => !data.is_empty(),
            Reply::Err(_) => true,
            Reply::Int(_) => true,
        }
    }
}
