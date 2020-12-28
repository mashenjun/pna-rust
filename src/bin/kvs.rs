// use clap::{App, Arg, SubCommand};
use std::process;
use structopt::StructOpt;


#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
#[structopt(author = env!("CARGO_PKG_AUTHORS"))]
#[structopt(about = "cli for in memory kv store")]
struct Cli {
    #[structopt(subcommand)]  // Note that we mark a field as a subcommand
    pub cmd: Option<Command>
}

#[derive(StructOpt, Debug)]
pub enum Command {
    #[structopt(name = "get")]
    Get {
        key: String,
    },
    #[structopt(name = "set")]
    Set {
        key: String,
        value: String,
    },
    #[structopt(name = "rm")]
    Remove {
        key: String,
    },
}

fn main() {
    let cli = Cli::from_args();

    match cli.cmd {
        None => process::exit(1),
        Some(c) => match c {
            Command::Set{key:_, value:_} => {
                eprintln!("unimplemented");
                process::exit(1);
            }
            Command::Get{key:_} => {
                eprintln!("unimplemented");
                process::exit(1);
            }
            Command::Remove{key:_} => {
                eprintln!("unimplemented");
                process::exit(1);
            }
        }
    }
}
