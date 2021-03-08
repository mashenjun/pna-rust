#[macro_use]
extern crate log;

use env_logger::Target;
use kvs::{KvsClient, Reply, Request, Result};
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
#[structopt(author = env!("CARGO_PKG_AUTHORS"))]
#[structopt(name = "kvs-client", about = "cli for kvs client")]
struct Client {
    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "get")]
    Get {
        #[structopt(name = "key")]
        key: String,
        #[structopt(
            long,
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "set")]
    Set {
        #[structopt(name = "key")]
        key: String,
        #[structopt(name = "value")]
        value: String,
        #[structopt(
            long,
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "rm")]
    Remove {
        #[structopt(name = "key")]
        key: String,
        #[structopt(
            long,
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
}

impl Command {
    pub fn to_request(&self) -> (Request, SocketAddr) {
            match self {
            Command::Get { key, addr } => (
                Request::Get {
                    key: key.to_string(),
                },
                *addr,
            ),
            Command::Set { key, value, addr } => (
                Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                },
                *addr,
            ),
            Command::Remove { key, addr } => (
                Request::Remove {
                    key: key.to_string(),
                },
                *addr,
            ),
        }
    }
}

fn run(client: &mut Client) -> Result<Reply> {
    let (req, addr) = client.cmd.to_request();
    let mut kv_client = KvsClient::new(addr)?;
    info!("connect to {}", addr);
    kv_client.process(&req)
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .target(Target::Stderr)
        .init();
    let mut client = Client::from_args();
    match run(&mut client) {
        Err(e) => {
            error!("{:?}", e);
            exit(1)
        }
        Ok(reply) => {
            if reply.should_println() {
                println!("{}", reply);
            }
            if let Reply::Err(data) = reply {
                error!("{:?}", data);
                exit(1);
            }
        }
    }
}
