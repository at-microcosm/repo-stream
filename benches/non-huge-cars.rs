extern crate repo_stream;
use repo_stream::DriverBuilder;

use criterion::{Criterion, criterion_group, criterion_main};

// use mimalloc::MiMalloc;
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

const EMPTY_CAR: &'static [u8] = include_bytes!("../car-samples/empty.car");
const TINY_CAR: &'static [u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &'static [u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &'static [u8] = include_bytes!("../car-samples/midsize.car");

pub fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Creating runtime failed");

    c.bench_function("empty-car", |b| {
        b.to_async(&rt).iter(async || drive_car(EMPTY_CAR).await)
    });
    c.bench_function("tiny-car", |b| {
        b.to_async(&rt).iter(async || drive_car(TINY_CAR).await)
    });
    c.bench_function("little-car", |b| {
        b.to_async(&rt).iter(async || drive_car(LITTLE_CAR).await)
    });
    c.bench_function("midsize-car", |b| {
        b.to_async(&rt).iter(async || drive_car(MIDSIZE_CAR).await)
    });
}

#[inline(always)]
fn ser(block: Vec<u8>) -> Vec<u8> {
    let s = block.len();
    usize::to_ne_bytes(s).to_vec()
}

async fn drive_car(bytes: &[u8]) -> usize {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(32)
        .with_block_processor(ser)
        .load_car(bytes)
        .await
        .unwrap();

    let mut n = 0;
    while let Some(pairs) = mem_car.next_chunk_strict(256).unwrap() {
        n += pairs.len();
    }
    n
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
