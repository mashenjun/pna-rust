use kvs::{Command, KvStore, KvsError, Result};
use std::env::current_dir;
use std::process;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
#[structopt(author = env!("CARGO_PKG_AUTHORS"))]
#[structopt(about = "cli for in memory kv store")]
struct Cli {
    #[structopt(subcommand)] // Note that we mark a field as a subcommand
    pub cmd: Option<Command>,
}

fn main() -> Result<()> {
    let cli = Cli::from_args();
    match run(&cli.cmd) {
        Err(err) => {
            eprintln!("run cmd {:?} err: {:?}", &cli.cmd, err);
            if let KvsError::KeyNotFoundError = err {
                println!("Key not found");
            }
            process::exit(1);
        }
        _ => Ok(()),
    }
}

fn run(cmd: &Option<Command>) -> Result<()> {
    match cmd {
        None => process::exit(1),
        Some(c) => {
            let mut store = KvStore::open(current_dir()?)?;
            match c {
                Command::Set { key: k, value: v } => {
                    store.set(k.to_string(), v.to_string())?;
                }
                Command::Get { key: k } => {
                    if let Some(s) = store.get(k.to_string())? {
                        println!("{}", s);
                    } else {
                        println!("Key not found");
                    }
                }
                Command::Remove { key: k } => {
                    store.remove(k.to_string())?;
                }
            }
        }
    }
    Ok(())
}
