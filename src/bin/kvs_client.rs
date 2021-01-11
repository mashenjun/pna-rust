#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use kvs::{Command, Result};
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
#[structopt(author = env!("CARGO_PKG_AUTHORS"))]
#[structopt(name = "kvs-client", about = "cli for kvs client")]
struct Client {
    #[structopt(
        long,
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    pub addr: SocketAddr,

    #[structopt(subcommand)]
    pub cmd: Command,
}

fn run(client: &mut Client) -> Result<()> {
    todo!();
    Ok(())
}

fn main() {
    let mut client = Client::from_args();
    if let Err(e) = run(&mut client) {
        error!("{:?}", e);
        exit(1)
    }
}
