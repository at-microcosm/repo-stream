extern crate repo_stream;
use repo_stream::DriverBuilder;
use std::path::{Path, PathBuf};

use criterion::{Criterion, criterion_group, criterion_main};

// use mimalloc::MiMalloc;
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

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

#[inline(always)]
fn ser(block: Vec<u8>) -> Vec<u8> {
    let s = block.len();
    usize::to_ne_bytes(s).to_vec()
}

async fn drive_car(filename: impl AsRef<Path>) -> usize {
    let reader = tokio::fs::File::open(filename).await.unwrap();
    let reader = tokio::io::BufReader::new(reader);

    let mut driver = DriverBuilder::new()
        .with_mem_limit_mb(1024)
        .with_block_processor(ser)
        .load_car(reader)
        .await
        .unwrap();

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).unwrap() {
        n += pairs.len();
    }
    n
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
