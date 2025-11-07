extern crate repo_stream;
use repo_stream::Driver;

use criterion::{Criterion, criterion_group, criterion_main};

const TINY_CAR: &'static [u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &'static [u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &'static [u8] = include_bytes!("../car-samples/midsize.car");

pub fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Creating runtime failed");

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

async fn drive_car(bytes: &[u8]) -> usize {
    let mut driver = match Driver::load_car(bytes, |block| block.len(), 32 * 2_usize.pow(20))
        .await
        .unwrap()
    {
        Driver::Lil(_, mem_driver) => mem_driver,
        Driver::Big(_) => panic!("not benching big cars here"),
    };

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).await.unwrap() {
        n += pairs.len();
    }
    n
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
