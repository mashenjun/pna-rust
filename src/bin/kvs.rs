extern crate clap;
use clap::{App, Arg, SubCommand};
use std::process;

fn main() {
    // access env variables
    let version: &str = env!("CARGO_PKG_VERSION");
    let author: &str = env!("CARGO_PKG_AUTHORS");

    let matches = App::new("kvs")
        .version(version) // todo read form toml
        .author(author)
        .about("cli for in memory kv store")
        .subcommand(
            SubCommand::with_name("get").about("get data").arg(
                Arg::with_name("key")
                    .required(true)
                    .help("get data by the given key"),
            ),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("set data")
                .arg(
                    Arg::with_name("key")
                        .required(true)
                        .help("data to set into"),
                )
                .arg(Arg::with_name("value").required(true).help("data to set")),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("remove data")
                .arg(Arg::with_name("key").required(true).help("remove data")),
        )
        .get_matches();

    match matches.subcommand() {
        ("get", Some(_m)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("set", Some(_m)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("rm", Some(_m)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => {
            process::exit(1);
        }
    }
}
