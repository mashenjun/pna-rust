use nom::bytes::complete::tag;
use nom::bytes::complete::{take, take_while, take_while1};
use nom::combinator::map_res;
use nom::error::{Error, ErrorKind};
use nom::sequence::{delimited, terminated, tuple};
use nom::{Err, IResult};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use structopt::StructOpt;

// TODO: impl Serialize and Deserialize for Request
// Request define the request in RESP format
#[derive(StructOpt, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
    #[structopt(name = "get")]
    Get { key: String },

    #[structopt(name = "set")]
    Set { key: String, value: String },

    #[structopt(name = "rm")]
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
pub enum Reply {
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

// norm impl
fn parse_single_line(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = terminated(take_while(|c| c != '\r' && c != '\n'), tag("\r\n"))(input)?;
    Ok((remain, Reply::SingleLine(res.to_string())))
}

fn parse_err(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = terminated(take_while1(|c| c != '\r' && c != '\n'), tag("\r\n"))(input)?;
    Ok((remain, Reply::Err(res.to_string())))
}

fn to_i64(input: &str) -> Result<i64, std::num::ParseIntError> {
    input.parse::<i64>()
}

fn parse_int(input: &str) -> IResult<&str, Reply> {
    let (remain, res) = terminated(
        map_res(take_while1(|c: char| c.is_digit(10) || c == '-'), to_i64),
        tag("\r\n"),
    )(input)?;
    Ok((remain, Reply::Int(res)))
}

pub fn parse_reply(input: &str) -> IResult<&str, Reply> {
    let (remain, prefix) = take(1usize)(input)?;
    match prefix {
        ":" => Ok(parse_int(remain)?),
        "+" => Ok(parse_single_line(remain)?),
        "-" => Ok(parse_err(remain)?),
        content => Err(Err::Error(Error::new(content, ErrorKind::Switch))),
    }
}

fn parse_arg(input: &str) -> IResult<&str, &str> {
    let (remain, res) = terminated(take_while1(|c| c != '\r' && c != '\n'), tag("\r\n"))(input)?;
    Ok((remain, res))
}

fn parse_get(input: &str) -> IResult<&str, Request> {
    let (remain, key) = parse_arg(input)?;
    Ok((
        remain,
        Request::Get {
            key: key.to_string(),
        },
    ))
}

fn parse_set(input: &str) -> IResult<&str, Request> {
    let (remain, (key, value)) = tuple((parse_arg, parse_arg))(input)?;
    Ok((
        remain,
        Request::Set {
            key: key.to_string(),
            value: value.to_string(),
        },
    ))
}

fn parse_remove(input: &str) -> IResult<&str, Request> {
    let (remain, key) = parse_arg(input)?;
    Ok((
        remain,
        Request::Remove {
            key: key.to_string(),
        },
    ))
}

pub fn parse_request(input: &str) -> IResult<&str, Request> {
    let (remain, _) = delimited(tag("*"), map_res(take(1usize), to_i64), tag("\r\n"))(input)?;
    let (remain, command) = parse_arg(remain)?;
    match command {
        "GET" => Ok(parse_get(remain)?),
        "SET" => Ok(parse_set(remain)?),
        "DEL" => Ok(parse_remove(remain)?),
        content => Err(Err::Error(Error::new(content, ErrorKind::Switch))),
    }
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
            let ret = super::parse_reply("-ERROR\r\n");
            check(ret, Reply::Err("ERROR".to_string()));
        }
        {
            let ret = super::parse_reply(":10\r\n");
            check(ret, Reply::Int(10));
        }
        {
            let ret = super::parse_reply("OK\r\n");
            assert!(matches!(ret, Err(_)));
        }
        {
            let ret = super::parse_reply(":OK\r\n");
            assert!(matches!(ret, Err(_)));
        }
    }

    #[test]
    fn parse_request() {
        let check = |ret: IResult<&str, Request>, target: Request| match ret {
            Err(_) => {
                panic!("wrong request");
            }
            Ok((_, request)) => {
                if request == target {
                    return;
                }
                panic!("wrong reply");
            }
        };
        {
            let req = Request::Get {
                key: "key".to_string(),
            };
            let input = req.to_resp();
            let ret = super::parse_request(input.as_str());
            check(ret, req)
        }
        {
            let req = Request::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            };
            let input = req.to_resp();
            let ret = super::parse_request(input.as_str());
            check(ret, req)
        }
        {
            let req = Request::Remove {
                key: "key".to_string(),
            };
            let input = req.to_resp();
            let ret = super::parse_request(input.as_str());
            check(ret, req)
        }
    }

    #[test]
    fn format_reply() {
        let s = Reply::SingleLine("OK".to_string()).to_string();
        assert_eq!(s, "OK")
    }
}
