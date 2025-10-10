extern crate repo_stream;
use futures::TryStreamExt;
use iroh_car::CarReader;
use std::convert::Infallible;

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

async fn drive_car(bytes: &[u8]) {
    let reader = CarReader::new(bytes).await.unwrap();

    let root = reader
        .header()
        .roots()
        .first()
        .ok_or("missing root")
        .unwrap()
        .clone();

    let stream = std::pin::pin!(reader.stream());

    let (_commit, v) =
        repo_stream::drive::Vehicle::init(root, stream, |block| Ok::<_, Infallible>(block.len()))
            .await
            .unwrap();
    let mut record_stream = std::pin::pin!(v.stream());

    while let Some(_) = record_stream.try_next().await.unwrap() {
        // just here for the drive
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
