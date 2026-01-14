use criterion::{Criterion, BatchSize, criterion_group, criterion_main};
use sha2::{Sha256, Digest};
use hmac_sha256::Hash;

pub fn compute(bytes: [u8; 32]) -> u32 {
    let mut zeros = 0;
    for byte in bytes {
        if byte == 0 {
            zeros += 8
        } else {
            zeros += byte.leading_zeros();
            break;
        }
    }
    zeros / 2
}

pub fn compute2(bytes: [u8; 32]) -> u32 {
    u128::from_be_bytes(bytes.split_at(16).0.try_into().unwrap())
        .leading_zeros() / 2
}

fn from_key_old(key: &[u8]) -> u32 {
    compute2(Sha256::digest(key).into())
}

fn from_key_new(key: &[u8]) -> u32 {
    compute2(Hash::hash(key).into())
}

pub fn criterion_benchmark(c: &mut Criterion) {
    for (name, case) in [
        ("no zeros",   [0xFF; 32]),
        ("two zeros",  [0x3F; 32]),
        ("some zeros", [0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
        ("many zeros", [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
    ] {
        let mut g = c.benchmark_group(name);
        g.bench_function("old", |b| {
            b.iter_batched(
                || case.clone(),
                |c| compute(c),
                BatchSize::SmallInput,
            )
        });
        g.bench_function("new", |b| {
            b.iter_batched(
                || case.clone(),
                |c| compute2(c),
                BatchSize::SmallInput,
            )
        });
    }

    for case in [
        "a",
        "aa",
        "aaa",
        "aaaa",
    ] {
        let mut g = c.benchmark_group(case);
        g.bench_function("old", |b| b.iter(|| from_key_old(case.as_bytes())));
        g.bench_function("new", |b| b.iter(|| from_key_new(case.as_bytes())));
    }
}


criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
