#[macro_use]
extern crate criterion;

use assert_cmd::prelude::*;
use criterion::Criterion;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use kvs::thread_pool::{self, *};
use kvs::{KvStore, KvsEngine, KvsServer, SledKvsEngine};
use predicates::str::is_empty;
use std::net::SocketAddr;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tempfile::TempDir;

fn write_queued_kvstore(c: &mut Criterion) {
    let inputs = gen_thread_cnt();
    c.bench_function_over_inputs(
        "write_queued_kvstore",
        |b, &num| {
            // setup workload
            let temp_dir = TempDir::new().unwrap();
            let pool = thread_pool::SharedQueueThreadPool::new(num).unwrap();
            let server = KvsServer::new(KvStore::open(temp_dir.path()).unwrap(), pool).unwrap();
            let arc_server = Arc::new(server);
            {
                let s = arc_server.clone();
                thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("{}", e);
                    }
                });
            }
            let (sender, receiver, mut handlers) = setup_set_workload();
            let sx = sender.clone();
            b.iter(move || {
                for i in 0..1000 {
                    sx.send(i).unwrap()
                }
                receiver.recv().unwrap();
            });
            // teardown
            for _ in 0..handlers.len() {
                sender.send(-1).unwrap()
            }
            for jh in handlers.drain(..) {
                jh.join().unwrap();
            }
            arc_server.shutdown().unwrap();
        },
        inputs,
    );
}

fn read_queued_kvstore(c: &mut Criterion) {
    let inputs = gen_thread_cnt();
    c.bench_function_over_inputs(
        "read_queued_kvstore",
        |b, &num| {
            // setup workload
            let temp_dir = TempDir::new().unwrap();
            let pool = thread_pool::SharedQueueThreadPool::new(num).unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            for i in 0..1000 {
                let key = format!("key{:0>3}", i);
                let value = key.clone();
                engine.set(key, value).unwrap();
            }
            let server = KvsServer::new(engine, pool).unwrap();
            let arc_server = Arc::new(server);
            {
                let s = arc_server.clone();
                thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("run error {}", e);
                    }
                });
            }

            let (sender, receiver, mut handlers) = setup_get_workload();
            let sx = sender.clone();
            b.iter(move || {
                for i in 0..1000 {
                    sx.send(i).unwrap()
                }
                receiver.recv().unwrap();
            });
            // teardown
            for _ in 0..handlers.len() {
                sender.send(-1).unwrap()
            }
            for jh in handlers.drain(..) {
                jh.join().unwrap();
            }
            arc_server.shutdown().unwrap();
        },
        inputs,
    );
}

fn write_rayon_kvstore(c: &mut Criterion) {
    let inputs = gen_thread_cnt();
    c.bench_function_over_inputs(
        "write_rayon_kvstore",
        |b, &num| {
            // setup workload
            let temp_dir = TempDir::new().unwrap();
            let pool = thread_pool::RayonThreadPool::new(num).unwrap();
            let server = KvsServer::new(KvStore::open(temp_dir.path()).unwrap(), pool).unwrap();
            let arc_server = Arc::new(server);
            {
                let s = arc_server.clone();
                thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("{}", e);
                    }
                });
            }
            let (sender, receiver, mut handlers) = setup_set_workload();
            let sx = sender.clone();
            b.iter(move || {
                for i in 0..1000 {
                    sx.send(i).unwrap()
                }
                receiver.recv().unwrap();
            });
            // teardown
            for _ in 0..handlers.len() {
                sender.send(-1).unwrap()
            }
            for jh in handlers.drain(..) {
                jh.join().unwrap();
            }
            arc_server.shutdown().unwrap();
        },
        inputs,
    );
}

fn read_rayon_kvstore(c: &mut Criterion) {
    let inputs = gen_thread_cnt();
    c.bench_function_over_inputs(
        "read_rayon_kvstore",
        |b, &num| {
            // setup workload
            let temp_dir = TempDir::new().unwrap();
            let pool = thread_pool::RayonThreadPool::new(num).unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            for i in 0..1000 {
                let key = format!("key{:0>3}", i);
                let value = key.clone();
                engine.set(key, value).unwrap();
            }
            let server = KvsServer::new(engine, pool).unwrap();
            let arc_server = Arc::new(server);
            {
                let s = arc_server.clone();
                thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("{}", e);
                    }
                });
            }

            let (sender, receiver, mut handlers) = setup_get_workload();
            let sx = sender.clone();
            b.iter(move || {
                for i in 0..1000 {
                    sx.send(i).unwrap()
                }
                receiver.recv().unwrap();
            });
            // teardown
            for _ in 0..handlers.len() {
                sender.send(-1).unwrap()
            }
            for jh in handlers.drain(..) {
                jh.join().unwrap();
            }
            arc_server.shutdown().unwrap();
        },
        inputs,
    );
}

fn write_rayon_sledkvengine(c: &mut Criterion) {
    let inputs = gen_thread_cnt();
    c.bench_function_over_inputs(
        "write_rayon_sledkvengine",
        |b, &num| {
            // setup workload
            let temp_dir = TempDir::new().unwrap();
            let pool = thread_pool::RayonThreadPool::new(num).unwrap();
            let server =
                KvsServer::new(SledKvsEngine::open(temp_dir.path()).unwrap(), pool).unwrap();
            let arc_server = Arc::new(server);
            {
                let s = arc_server.clone();
                thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("run error {}", e);
                    }
                });
            }
            let (sender, receiver, mut handlers) = setup_set_workload();
            let sx = sender.clone();
            b.iter(move || {
                for i in 0..1000 {
                    sx.send(i).unwrap()
                }
                receiver.recv().unwrap();
            });
            // teardown
            for _ in 0..handlers.len() {
                sender.send(-1).unwrap()
            }
            for jh in handlers.drain(..) {
                jh.join().unwrap();
            }
            arc_server.shutdown().unwrap();
        },
        inputs,
    );
}

fn read_rayon_sledkvengine(c: &mut Criterion) {
    let inputs = gen_thread_cnt();
    c.bench_function_over_inputs(
        "read_rayon_sledkvengine",
        |b, &num| {
            // setup workload
            let temp_dir = TempDir::new().unwrap();
            let pool = thread_pool::RayonThreadPool::new(num).unwrap();
            let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
            for i in 0..1000 {
                let key = format!("key{:0>3}", i);
                let value = key.clone();
                engine.set(key, value).unwrap();
            }
            let server = KvsServer::new(engine, pool).unwrap();
            let arc_server = Arc::new(server);
            {
                let s = arc_server.clone();
                thread::spawn(move || {
                    if let Err(e) = s.run(SocketAddr::from_str("127.0.0.1:4000").unwrap()) {
                        return println!("run error {}", e);
                    }
                });
            }

            let (sender, receiver, mut handlers) = setup_get_workload();
            let sx = sender.clone();
            b.iter(move || {
                for i in 0..1000 {
                    sx.send(i).unwrap()
                }
                receiver.recv().unwrap();
            });
            // teardown
            for _ in 0..handlers.len() {
                sender.send(-1).unwrap()
            }
            for jh in handlers.drain(..) {
                jh.join().unwrap();
            }
            arc_server.shutdown().unwrap();
        },
        inputs,
    );
}

fn gen_thread_cnt() -> Vec<u32> {
    let mut cnt = vec![1];
    for i in 1..num_cpus::get() as u32 {
        cnt.push(i * 2);
    }
    return cnt;
}

fn setup_set_workload() -> (Sender<i32>, Receiver<i32>, Vec<JoinHandle<()>>) {
    let (sender_job, receiver_job) = bounded::<i32>(num_cpus::get());
    let (sender_done, receiver_done) = unbounded::<i32>();
    let mut handlers = Vec::new();

    for _ in 0..num_cpus::get() {
        let rx = receiver_job.clone();
        let sx = sender_done.clone();
        let handle = thread::spawn(move || {
            execute_set(rx, sx);
        });
        handlers.push(handle);
    }
    (sender_job, receiver_done, handlers)
}

fn setup_get_workload() -> (Sender<i32>, Receiver<i32>, Vec<JoinHandle<()>>) {
    let (sender_job, receiver_job) = bounded::<i32>(num_cpus::get());
    let (sender_done, receiver_done) = unbounded::<i32>();
    let mut handlers = Vec::new();
    for _ in 0..1000 {
        let rx = receiver_job.clone();
        let sx = sender_done.clone();
        let handle = thread::spawn(move || {
            execute_get(rx, sx);
        });
        handlers.push(handle);
    }
    (sender_job, receiver_done, handlers)
}

fn execute_set(rx: Receiver<i32>, sx: Sender<i32>) {
    loop {
        match rx.recv() {
            Ok(id) => {
                if id < 0 {
                    return;
                }
                let key = format!("key{:0>3}", id);
                Command::cargo_bin("kvs-client")
                    .unwrap()
                    .args(&["set", key.as_str(), "value", "--addr", "127.0.0.1:4000"])
                    .assert()
                    .success()
                    .stdout(is_empty());
                if id >= 999 {
                    sx.send(id).unwrap();
                }
            }
            Err(e) => {
                println!("rx.recv() error: {:?}", e);
            }
        }
    }
}

fn execute_get(rx: Receiver<i32>, sx: Sender<i32>) {
    loop {
        match rx.recv() {
            Ok(id) => {
                if id < 0 {
                    return;
                }
                let key = format!("key{:0>3}", id);
                let value = format!("key{:0>3}\n", id);
                Command::cargo_bin("kvs-client")
                    .unwrap()
                    .args(&["get", key.as_str(), "--addr", "127.0.0.1:4000"])
                    .assert()
                    .success()
                    .stdout(value);
                if id >= 999 {
                    sx.send(id).unwrap();
                }
            }
            Err(e) => {
                println!("rx.recv() error: {:?}", e);
            }
        }
    }
}

criterion_group!(
    benches,
    write_queued_kvstore,
    read_queued_kvstore,
    write_rayon_kvstore,
    read_rayon_kvstore,
    write_rayon_sledkvengine,
    read_rayon_sledkvengine
);
criterion_main!(benches);
