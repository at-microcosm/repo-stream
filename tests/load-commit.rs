extern crate repo_stream;
use cid::Cid;
use iroh_car::{CarHeader, CarReader, CarWriter};
use repo_stream::{DriverBuilder, LoadCommitError};

const EMPTY_CAR: &[u8] = include_bytes!("../car-samples/empty.car");
const TINY_CAR: &[u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &[u8] = include_bytes!("../car-samples/little.car");
const SLICE_ONE_CAR: &[u8] = include_bytes!("../car-samples/slice-one.car");

/// `load_commit` should return the same commit the full loader parses, and in
/// doing so exercises CID verification against a real commit block.
async fn assert_commit_matches_full_load(bytes: &[u8]) {
    let mem_car = DriverBuilder::new()
        .load_car(bytes)
        .await
        .expect("should load into memory");
    let expected = mem_car.commit;

    let commit = DriverBuilder::new()
        .load_commit(bytes)
        .await
        .expect("should read the commit");

    assert_eq!(commit.did, expected.did);
    assert_eq!(commit.version, expected.version);
    assert_eq!(commit.rev, expected.rev);
    assert_eq!(commit.data, expected.data);
    assert_eq!(commit.prev, expected.prev);
    assert_eq!(commit.sig, expected.sig);
}

#[tokio::test]
async fn load_commit_matches_full_load_empty() {
    assert_commit_matches_full_load(EMPTY_CAR).await;
}

#[tokio::test]
async fn load_commit_matches_full_load_tiny() {
    assert_commit_matches_full_load(TINY_CAR).await;
}

#[tokio::test]
async fn load_commit_matches_full_load_little() {
    assert_commit_matches_full_load(LITTLE_CAR).await;
}

/// The `#sync` use case: pull the commit out of a CAR slice that doesn't carry
/// every block.
#[tokio::test]
async fn load_commit_reads_a_slice() {
    let commit = DriverBuilder::new()
        .load_commit(SLICE_ONE_CAR)
        .await
        .expect("should read the commit from a slice");

    assert_eq!(commit.version, 3, "atproto repo commit version");
    assert!(commit.did.starts_with("did:"), "did: {}", commit.did);
}

/// Canonical atproto CID (cidv1 / dag-cbor / sha-256) for some bytes.
fn atproto_cid(bytes: &[u8]) -> Cid {
    use cid::multihash::Multihash;
    use sha2::{Digest, Sha256};
    let mh = Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    Cid::new_v1(0x71, mh)
}

/// Pull the (root cid, commit bytes) out of a real CAR.
async fn commit_block(car: &[u8]) -> (Cid, Vec<u8>) {
    let mut reader = CarReader::new(car).await.unwrap();
    let root = reader.header().roots()[0];
    while let Some((cid, data)) = reader.next_block().await.unwrap() {
        if cid == root {
            return (root, data);
        }
    }
    panic!("sample CAR had no commit block");
}

/// Build a CAR with `filler` blocks (each under its real CID) ahead of the commit.
async fn car_with_junk_before_commit(filler: &[Vec<u8>], root: Cid, commit: &[u8]) -> Vec<u8> {
    let mut writer = CarWriter::new(CarHeader::new_v1(vec![root]), Vec::new());
    for block in filler {
        writer.write(atproto_cid(block), block).await.unwrap();
    }
    writer.write(root, commit).await.unwrap();
    writer.finish().await.unwrap()
}

/// Hitting the read budget before the commit yields a resumable `PartialCommit`
/// that continues the commit hunt (not a repo load) with a raised budget.
#[tokio::test]
async fn read_budget_exceeded_resumes_to_the_commit() {
    let (root, commit_bytes) = commit_block(TINY_CAR).await;
    let expected = DriverBuilder::new()
        .load_commit(TINY_CAR)
        .await
        .expect("commit from tiny.car");

    // Three 700 KB filler blocks ahead of the commit. A 1 MiB budget trips after
    // the second, leaving the third to be streamed past on resume — so this also
    // exercises the carried-over byte count.
    let filler: Vec<Vec<u8>> = (0..3u8).map(|i| vec![i; 700_000]).collect();
    let car = car_with_junk_before_commit(&filler, root, &commit_bytes).await;

    let partial = match DriverBuilder::new()
        .with_mem_limit_mb(1)
        .load_commit(car.as_slice())
        .await
    {
        Err(LoadCommitError::ReadBudgetExceeded(p)) => p,
        other => panic!("expected ReadBudgetExceeded, got {other:?}"),
    };
    assert!(partial.read >= partial.budget, "tripped past the budget");

    let resumed = partial
        .continue_loading(64)
        .await
        .expect("resume finds the commit");

    assert_eq!(resumed.did, expected.did);
    assert_eq!(resumed.rev, expected.rev);
    assert_eq!(resumed.data, expected.data);
}
