use criterion::{Criterion, criterion_group, criterion_main};
use multihash_codetable::{Code, MultihashDigest};
use cid::Cid;
use sha2::{Digest, Sha256};

fn multihash_verify(given: Cid, block: &[u8]) -> bool {
    let calculated = Cid::new_v1(0x71, Code::Sha2_256.digest(block));
    calculated == given
}

fn effortful_verify(given: Cid, block: &[u8]) -> bool {
    // we know we're in atproto, so we can make a few assumptions
    if given.version() != cid::Version::V1 {
        return false;
    }
    let (codec, given_digest, _) = given.hash().into_inner();
    if codec != 0x12 {
        return false;
    }
    given_digest[..32] == *Sha256::digest(block)
}

fn fastloose_verify(given: Cid, block: &[u8]) -> bool {
    let (_, given_digest, _) = given.hash().into_inner();
    given_digest[..32] == *Sha256::digest(block)
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let some_bytes: Vec<u8> = vec![0x1a, 0x00, 0xAA, 0x39, 0x8C].repeat(100);
    let cid = Cid::new_v1(0x71, Code::Sha2_256.digest(&some_bytes));

    let mut g = c.benchmark_group("CID check");
    g.bench_function("multihash", |b| b.iter(|| multihash_verify(cid, &some_bytes)));
    g.bench_function("effortful", |b| b.iter(|| effortful_verify(cid, &some_bytes)));
    g.bench_function("fastloose", |b| b.iter(|| fastloose_verify(cid, &some_bytes)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
