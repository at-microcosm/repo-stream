extern crate repo_stream;
use repo_stream::DriverBuilder;
use std::collections::HashSet;
use std::path::Path;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const EMPTY_CAR: &[u8] = include_bytes!("../car-samples/empty.car");
const TINY_CAR: &[u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &[u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &[u8] = include_bytes!("../car-samples/midsize.car");

/// Walk every record and collect unique collection prefixes via HashSet dedup.
async fn collect_naive(bytes: &[u8]) -> Vec<String> {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(100)
        .load_car(bytes)
        .await
        .unwrap();

    let mut seen = HashSet::new();
    let mut collections = vec![];
    while let Some(outputs) = mem_car.next_chunk_strict(256).unwrap() {
        for output in outputs {
            let collection = output.key.split_once('/').unwrap().0.to_string();
            if seen.insert(collection.clone()) {
                collections.push(collection);
            }
        }
    }
    collections
}

/// Seek past each collection using a sentinel that sorts strictly after any valid key
/// in the collection. Atproto rkeys are capped at 512 chars; 513 tildes exceeds that
/// maximum, so `collection/<513 tildes>` can never equal an actual record key and
/// is guaranteed to be greater than `collection/<512 tildes>` (the max valid key).
async fn collect_seeking(bytes: &[u8]) -> Vec<String> {
    // 513 > max rkey length (512), so this is strictly greater than any valid key
    let tilde_max = "~".repeat(513);
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(100)
        .load_car(bytes)
        .await
        .unwrap();

    let mut collections = vec![];
    loop {
        match mem_car.next_strict().unwrap() {
            None => break,
            Some(output) => {
                let collection = output.key.split_once('/').unwrap().0.to_string();
                collections.push(collection.clone());
                mem_car.seek(&format!("{collection}/{tilde_max}")).unwrap();
            }
        }
    }
    collections
}

async fn collect_naive_file(path: &Path) -> Vec<String> {
    let reader = tokio::io::BufReader::new(tokio::fs::File::open(path).await.unwrap());
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(1024)
        .load_car(reader)
        .await
        .unwrap();

    let mut seen = HashSet::new();
    let mut collections = vec![];
    while let Some(outputs) = mem_car.next_chunk_strict(256).unwrap() {
        for output in outputs {
            let collection = output.key.split_once('/').unwrap().0.to_string();
            if seen.insert(collection.clone()) {
                collections.push(collection);
            }
        }
    }
    collections
}

async fn collect_seeking_file(path: &Path) -> Vec<String> {
    let tilde_max = "~".repeat(513);
    let reader = tokio::io::BufReader::new(tokio::fs::File::open(path).await.unwrap());
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(1024)
        .load_car(reader)
        .await
        .unwrap();

    let mut collections = vec![];
    loop {
        match mem_car.next_strict().unwrap() {
            None => break,
            Some(output) => {
                let collection = output.key.split_once('/').unwrap().0.to_string();
                collections.push(collection.clone());
                mem_car.seek(&format!("{collection}/{tilde_max}")).unwrap();
            }
        }
    }
    collections
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Creating runtime failed");

    let cars = [
        ("empty", EMPTY_CAR),
        ("tiny", TINY_CAR),
        ("little", LITTLE_CAR),
        ("midsize", MIDSIZE_CAR),
    ];

    let mut group = c.benchmark_group("collections");

    for (name, bytes) in cars {
        // Sanity-check: both approaches must return the same collections
        let naive = rt.block_on(collect_naive(bytes));
        let mut seeking = rt.block_on(collect_seeking(bytes));
        seeking.sort();
        let mut naive_sorted = naive.clone();
        naive_sorted.sort();
        assert_eq!(naive_sorted, seeking, "approaches disagree for {name}");
        println!("{name}: {naive_sorted:?}");

        group.bench_with_input(BenchmarkId::new("naive", name), bytes, |b, bytes| {
            b.to_async(&rt).iter(async || collect_naive(bytes).await)
        });
        group.bench_with_input(BenchmarkId::new("seeking", name), bytes, |b, bytes| {
            b.to_async(&rt).iter(async || collect_seeking(bytes).await)
        });
    }

    group.finish();

    if let Ok(huge_car) = std::env::var("HUGE_CAR") {
        let path: std::path::PathBuf = huge_car.into();

        // Sanity-check the huge car too
        let naive = rt.block_on(collect_naive_file(&path));
        let mut seeking = rt.block_on(collect_seeking_file(&path));
        seeking.sort();
        let mut naive_sorted = naive.clone();
        naive_sorted.sort();
        assert_eq!(naive_sorted, seeking, "approaches disagree for huge-car");
        println!("huge: {naive_sorted:?}");

        let mut group = c.benchmark_group("collections-huge");

        group.bench_with_input(BenchmarkId::new("naive", "huge"), &path, |b, path| {
            b.to_async(&rt)
                .iter(async || collect_naive_file(path).await)
        });
        group.bench_with_input(BenchmarkId::new("seeking", "huge"), &path, |b, path| {
            b.to_async(&rt)
                .iter(async || collect_seeking_file(path).await)
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
