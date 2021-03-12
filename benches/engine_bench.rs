#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Benchmark, Criterion, ParameterizedBenchmark};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use sled::open;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let bench = Benchmark::new(
        "kvs",
        |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
                },
                |(store, _temp_dir)| {
                    for i in 1..(1 << 12) {
                        store.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        // iter::once(()),
    )
    .with_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (SledKvsEngine::new(open(&temp_dir).unwrap()), temp_dir)
            },
            |(db, _temp_dir)| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        },
        vec![8, 12, 16, 20],
    )
    .with_function("sled", |b, i| {
        let temp_dir = TempDir::new().unwrap();
        let db = SledKvsEngine::new(open(&temp_dir).unwrap());
        for key_i in 1..(1 << i) {
            db.set(format!("key{}", key_i), "value".to_string())
                .unwrap();
        }
        let mut rng = SmallRng::from_seed([0; 16]);
        b.iter(|| {
            db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
        })
    });
    c.bench("get_bench", bench);
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
