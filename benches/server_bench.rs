#[macro_use]
extern crate criterion;

use assert_cmd::prelude::*;
use criterion::Criterion;
use crossbeam::channel::{unbounded, Receiver, Sender};
use kvs::thread_pool::{self, *};
use kvs::{KvStore, KvsServer};
use std::net::SocketAddr;
use std::os::raw::c_int;
use std::os::unix::thread::JoinHandleExt;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

const SIGNAL_INT: c_int = libc::SIGINT;

fn write_queued_kvstore(c: &mut Criterion) {
    // TODO hard to stop the tcp server in a blocking style
    //      wrong implementation
    let inputs = &[1, 2];
    c.bench_function_over_inputs(
        "kvs",
        |b, &&num| {
            // unable to implement the benchmark in an blocking style
            // setup
            println!("setup");
            let temp_dir = TempDir::new().unwrap();

            let (sender1, receiver1) = unbounded::<i32>();
            let (sender2, receiver2) = unbounded::<i32>();

            let pool = thread_pool::SharedQueueThreadPool::new(num).unwrap();
            let server = KvsServer::new(KvStore::open(temp_dir.path()).unwrap(), pool);
            let arc_server = Arc::new(server);
            let tid: libc::pthread_t;
            {
                let s = arc_server.clone();
                let jh = thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("{}", e);
                    }
                });
                tid = jh.as_pthread_t();
            }

            let mut handlers = Vec::new();
            println!("setup");
            for _ in 0..4 {
                let rx = receiver1.clone();
                let sx = sender2.clone();
                let handle = thread::spawn(move || {
                    execute(rx, sx);
                });
                handlers.push(handle);
            }
            println!("setup");
            b.iter(move || {
                for i in 0..1003 {
                    sender1.send(i).unwrap()
                }
                receiver2.recv().unwrap();
                unsafe { libc::pthread_kill(tid, SIGNAL_INT) };
            });
        },
        inputs,
    );
}

fn execute(rx: Receiver<i32>, sx: Sender<i32>) {
    loop {
        match rx.recv() {
            Ok(id) => {
                let key = format!("key{:0>3}", id);
                Command::cargo_bin("kvs-client")
                    .unwrap()
                    .args(&["set", key.as_str(), "value", "--addr", "127.0.0.1:4000"])
                    .assert()
                    .success();
                if id >= 1000 {
                    sx.send(id).unwrap();
                }
            }
            Err(_) => println!("exit thread"),
        }
    }
}

criterion_group!(benches, write_queued_kvstore);
criterion_main!(benches);
