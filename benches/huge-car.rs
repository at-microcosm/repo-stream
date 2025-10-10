extern crate repo_stream;
use futures::TryStreamExt;
use iroh_car::CarReader;
use std::convert::Infallible;
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

async fn drive_car(filename: impl AsRef<Path>) {
    let reader = tokio::fs::File::open(filename).await.unwrap();
    let reader = tokio::io::BufReader::new(reader);
    let reader = CarReader::new(reader).await.unwrap();

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
