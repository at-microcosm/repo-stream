//! Benchmark comparing two key-only walk implementations:
//!
//! - **Approach A (filter)**: call `next_chunk_with_nodes` and discard Node items at the
//!   call site. The underlying walker emits and clones node bytes that are immediately
//!   thrown away.
//! - **Approach B (separate)**: call `next_chunk` which routes through `Walker::step`,
//!   a separate code path that never allocates node bytes.
//!
//! The difference quantifies the cost of `data.clone()` inside `step_with_nodes`.

extern crate repo_stream;
use repo_stream::{DriverBuilder, WalkItem};
use std::path::Path;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const EMPTY_CAR: &[u8] = include_bytes!("../car-samples/empty.car");
const TINY_CAR: &[u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &[u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &[u8] = include_bytes!("../car-samples/midsize.car");

/// Approach A: key-only walk via filter over the node-inclusive path.
/// Calls `next_chunk_with_nodes`, counts records, discards Node items.
async fn count_records_filter(bytes: &[u8]) -> usize {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(100)
        .load_car(bytes)
        .await
        .unwrap();

    let mut records = 0usize;
    while let Some(items) = mem_car.next_chunk_with_nodes(256).unwrap() {
        for item in items {
            if matches!(item, WalkItem::Record(_)) {
                records += 1;
            }
        }
    }
    records
}

/// Approach B: key-only walk via the separate `next_chunk` path.
/// `Walker::step` never allocates node bytes.
async fn count_records_separate(bytes: &[u8]) -> usize {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(100)
        .load_car(bytes)
        .await
        .unwrap();

    let mut records = 0usize;
    while let Some(chunk) = mem_car.next_chunk_strict(256).unwrap() {
        records += chunk.len();
    }
    records
}

/// Walk with nodes: use `next_chunk_with_nodes`, count both records and nodes.
async fn count_records_and_nodes(bytes: &[u8]) -> (usize, usize) {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(100)
        .load_car(bytes)
        .await
        .unwrap();

    let mut records = 0usize;
    let mut nodes = 0usize;
    while let Some(items) = mem_car.next_chunk_with_nodes(256).unwrap() {
        for item in items {
            match item {
                WalkItem::Record(_) => records += 1,
                WalkItem::Node { .. } => nodes += 1,
                _ => {}
            }
        }
    }
    (records, nodes)
}

async fn count_records_filter_file(path: &Path) -> usize {
    let reader = tokio::io::BufReader::new(tokio::fs::File::open(path).await.unwrap());
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(1024)
        .load_car(reader)
        .await
        .unwrap();

    let mut records = 0usize;
    while let Some(items) = mem_car.next_chunk_with_nodes(256).unwrap() {
        for item in items {
            if matches!(item, WalkItem::Record(_)) {
                records += 1;
            }
        }
    }
    records
}

async fn count_records_separate_file(path: &Path) -> usize {
    let reader = tokio::io::BufReader::new(tokio::fs::File::open(path).await.unwrap());
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(1024)
        .load_car(reader)
        .await
        .unwrap();

    let mut records = 0usize;
    while let Some(chunk) = mem_car.next_chunk_strict(256).unwrap() {
        records += chunk.len();
    }
    records
}

async fn count_records_and_nodes_file(path: &Path) -> (usize, usize) {
    let reader = tokio::io::BufReader::new(tokio::fs::File::open(path).await.unwrap());
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(1024)
        .load_car(reader)
        .await
        .unwrap();

    let mut records = 0usize;
    let mut nodes = 0usize;
    while let Some(items) = mem_car.next_chunk_with_nodes(256).unwrap() {
        for item in items {
            match item {
                WalkItem::Record(_) => records += 1,
                WalkItem::Node { .. } => nodes += 1,
                _ => {}
            }
        }
    }
    (records, nodes)
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

    // Sanity-check: both approaches agree on record count.
    for (name, bytes) in cars {
        let a = rt.block_on(count_records_filter(bytes));
        let b = rt.block_on(count_records_separate(bytes));
        assert_eq!(a, b, "approaches disagree on record count for {name}");
        let (records, nodes) = rt.block_on(count_records_and_nodes(bytes));
        println!("{name}: {records} records, {nodes} nodes");
    }

    let mut group = c.benchmark_group("node-counts");

    for (name, bytes) in cars {
        group.bench_with_input(
            BenchmarkId::new("records-filter-approach", name),
            bytes,
            |b, bytes| {
                b.to_async(&rt)
                    .iter(async || count_records_filter(bytes).await)
            },
        );
        group.bench_with_input(
            BenchmarkId::new("records-separate-approach", name),
            bytes,
            |b, bytes| {
                b.to_async(&rt)
                    .iter(async || count_records_separate(bytes).await)
            },
        );
        group.bench_with_input(
            BenchmarkId::new("records-and-nodes", name),
            bytes,
            |b, bytes| {
                b.to_async(&rt)
                    .iter(async || count_records_and_nodes(bytes).await)
            },
        );
    }

    group.finish();

    if let Ok(huge_car) = std::env::var("HUGE_CAR") {
        let path: std::path::PathBuf = huge_car.into();

        // Sanity-check the huge car too.
        let a = rt.block_on(count_records_filter_file(&path));
        let b = rt.block_on(count_records_separate_file(&path));
        assert_eq!(a, b, "approaches disagree on record count for huge-car");
        let (records, nodes) = rt.block_on(count_records_and_nodes_file(&path));
        println!("huge: {records} records, {nodes} nodes");

        let mut group = c.benchmark_group("node-counts-huge");

        group.bench_with_input(
            BenchmarkId::new("records-filter-approach", "huge"),
            &path,
            |b, path| {
                b.to_async(&rt)
                    .iter(async || count_records_filter_file(path).await)
            },
        );
        group.bench_with_input(
            BenchmarkId::new("records-separate-approach", "huge"),
            &path,
            |b, path| {
                b.to_async(&rt)
                    .iter(async || count_records_separate_file(path).await)
            },
        );
        group.bench_with_input(
            BenchmarkId::new("records-and-nodes", "huge"),
            &path,
            |b, path| {
                b.to_async(&rt)
                    .iter(async || count_records_and_nodes_file(path).await)
            },
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
