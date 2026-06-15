extern crate repo_stream;
use repo_stream::DriverBuilder;

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
