extern crate repo_stream;
use repo_stream::drive::Processable;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use criterion::{Criterion, criterion_group, criterion_main};

#[derive(Clone, Serialize, Deserialize)]
struct S(usize);

impl Processable for S {
    fn get_size(&self) -> usize {
        0 // no additional space taken, just its stack size (newtype is free)
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Creating runtime failed");

    let filename = std::env::var("HUGE_CAR").expect("HUGE_CAR env var");
    let filename: PathBuf = filename.try_into().unwrap();

    c.bench_function("huge-car", |b| {
        b.to_async(&rt).iter(async || drive_car(&filename).await)
    });
}

async fn drive_car(filename: impl AsRef<Path>) -> usize {
    let reader = tokio::fs::File::open(filename).await.unwrap();
    let reader = tokio::io::BufReader::new(reader);

    let mb = 2_usize.pow(20);

    let mut driver = match repo_stream::drive::load_car(reader, |block| S(block.len()), 1024 * mb)
        .await
        .unwrap()
    {
        repo_stream::drive::Vehicle::Lil(_, mem_driver) => mem_driver,
        repo_stream::drive::Vehicle::Big(_) => panic!("not doing disk for benchmark"),
    };

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).await.unwrap() {
        n += pairs.len();
    }
    n
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
