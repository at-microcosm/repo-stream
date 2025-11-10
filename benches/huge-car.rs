extern crate repo_stream;
use repo_stream::Driver;
use std::path::{Path, PathBuf};

use criterion::{Criterion, criterion_group, criterion_main};

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

    let mut driver = match Driver::load_car(reader, |block| block.len(), 1024)
        .await
        .unwrap()
    {
        Driver::Memory(_, mem_driver) => mem_driver,
        Driver::Disk(_) => panic!("not doing disk for benchmark"),
    };

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).await.unwrap() {
        n += pairs.len();
    }
    n
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
