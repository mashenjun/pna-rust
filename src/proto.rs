use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::{take_while, take_while1, take_while_m_n};
use nom::combinator::{map, map_res};
use nom::error::{Error, ErrorKind};
use nom::multi::many_m_n;
use nom::sequence::delimited;
use nom::{Err, IResult};

use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
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
            Request::Get { key } => String::from(format!("*2\r\n{}\r\n{}\r\n", "GET", key)),
            Request::Set { key, value } => {
                String::from(format!("*3\r\n{}\r\n{}\r\n{}\r\n", "SET", key, value))
            }
            Request::Remove { key } => String::from(format!("*2\r\n{}\r\n{}\r\n", "DEL", key)),
        };
        s
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Reply {
    SingleLine(String),
    Err(String),
    Int(i64),
}

fn parse_single_line(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = delimited(
        tag("+"),
        take_while(|c| c != '\r' && c != '\n'),
        tag("\r\n"),
    )(input)?;
    Ok((remain, Reply::SingleLine(res.to_string())))
}

fn parse_err(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = delimited(
        tag("-"),
        take_while1(|c| c != '\r' && c != '\n'),
        tag("\r\n"),
    )(input)?;
    Ok((remain, Reply::Err(res.to_string())))
}

fn to_i64(input: &str) -> Result<i64, std::num::ParseIntError> {
    i64::from_str_radix(input, 10)
}

fn parse_int(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = delimited(
        tag(":"),
        map_res(take_while1(|c: char| c.is_digit(10) || c == '-'), to_i64),
        tag("\r\n"),
    )(input)?;
    Ok((remain, Reply::Int(res)))
}

fn parse_reply(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = alt((parse_single_line, parse_err, parse_int))(input)?;
    Ok((remain, res))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_to_resp() {
        {
            let cmd = Request::Get {
                key: "key".to_string(),
            };
            assert_eq!(cmd.to_resp(), "*2\r\nGET\r\nkey\r\n".to_string())
        }
        {
            let cmd = Request::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            };
            assert_eq!(cmd.to_resp(), "*3\r\nSET\r\nkey\r\nvalue\r\n".to_string())
        }
        {
            let cmd = Request::Remove {
                key: "key".to_string(),
            };
            assert_eq!(cmd.to_resp(), "*2\r\nDEL\r\nkey\r\n".to_string())
        }
    }

    #[test]
    fn parse_reply() {
        let check = |ret: IResult<&str, Reply>, target: Reply| match ret {
            Err(_) => {
                panic!("wrong reply");
            }
            Ok((_, reply)) => {
                if reply == target {
                    return;
                }
                panic!("wrong reply");
            }
        };
        {
            let ret = super::parse_reply("+OK\r\n");
            check(ret, Reply::SingleLine("OK".to_string()));
        }
        {
            let ret = super::parse_reply("-ERROE\r\n");
            check(ret, Reply::Err("ERROE".to_string()));
        }
        {
            let ret = super::parse_reply(":10\r\n");
            check(ret, Reply::Int(10));
        }
        {
            let ret = parse_single_line("OK\r\n");
            assert!(matches!(ret, Err(_)));
        }
        {
            let ret = parse_single_line(":OK\r\n");
            assert!(matches!(ret, Err(_)));
        }
    }
}
