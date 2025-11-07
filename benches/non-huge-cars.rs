extern crate repo_stream;

use criterion::{Criterion, criterion_group, criterion_main};
use repo_stream::drive::Processable;
use serde::{Deserialize, Serialize};

const TINY_CAR: &'static [u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &'static [u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &'static [u8] = include_bytes!("../car-samples/midsize.car");

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
    let mut driver =
        match repo_stream::drive::load_car(bytes, |block| S(block.len()), 32 * 2_usize.pow(20))
            .await
            .unwrap()
        {
            repo_stream::drive::Vehicle::Lil(_, mem_driver) => mem_driver,
            repo_stream::drive::Vehicle::Big(_) => panic!("not benching big cars here"),
        };

    let mut n = 0;
    while let Some(pairs) = driver.next_chunk(256).await.unwrap() {
        n += pairs.len();
    }
    n
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
