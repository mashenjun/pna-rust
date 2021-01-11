#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use env_logger::{Builder, Target};
use kvs::*;
use std::env::current_dir;
use std::error::Error;
use std::fs;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::exit;
use structopt::StructOpt;

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum EngineOpt {
        kvs,
        sled
    }
}

const DEFAULT_ENGINE: EngineOpt = EngineOpt::kvs;
const KVS_ENGINE_FILE: &str = "kvs.engine";
const SLED_ENGINE_FILE: &str = "sled.engine";

#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
#[structopt(author = env!("CARGO_PKG_AUTHORS"))]
#[structopt(name = "kvs-server", about = "cli for kvs server")]
struct Server {
    #[structopt(
        long,
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    pub addr: SocketAddr,

    #[structopt(long, value_name = "ENGINE-NAME", possible_values=&EngineOpt::variants())]
    pub engine: Option<EngineOpt>,
}

impl Server {
    fn validate(&mut self) {
        match (self.engine, check_old_engine()) {
            (None, old) => self.engine = old,
            (Some(curr), Some(old)) => {
                if curr != old {
                    error!("Wrong engine!");
                    exit(1);
                }
            }
            _ => {}
        }
    }
}

fn run(srv: &mut Server) -> Result<()> {
    srv.validate();
    let opt = srv.engine.unwrap_or(DEFAULT_ENGINE);
    info!("version {}", env!("CARGO_PKG_VERSION"));
    info!("engine: {}", opt);
    info!("listening on {}", srv.addr);
    match opt {
        EngineOpt::kvs => {
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(KVS_ENGINE_FILE)?;
            todo!()
        }
        EngineOpt::sled => {
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(SLED_ENGINE_FILE)?;
            todo!()
        }
    }
    Ok(())
}

fn main() {
    env_logger::builder().target(Target::Stdout).init();
    let mut srv = Server::from_args();
    if let Err(e) = run(&mut srv) {
        error!("{:?}", e);
        exit(1)
    }
}

fn check_old_engine() -> Option<EngineOpt> {
    if Path::new(KVS_ENGINE_FILE).exists() {
        return Some(EngineOpt::kvs);
    }
    if Path::new(SLED_ENGINE_FILE).exists() {
        return Some(EngineOpt::sled);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_opt_format() {
        println!("{}", EngineOpt::kvs);
        println!("{}", EngineOpt::sled);
    }
}
