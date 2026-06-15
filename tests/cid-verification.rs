extern crate repo_stream;
use repo_stream::{DriverBuilder, LoadError};

const TINY_CAR: &[u8] = include_bytes!("../car-samples/tiny.car");

/// Verification is on for every block read, with no opt-out: a block whose bytes
/// were tampered with — but whose CAR framing is intact — must be rejected.
///
/// `load_commit` shares the same `verify_block_cid` path, so this covers it too.
#[tokio::test]
async fn load_car_rejects_a_corrupted_block() {
    let mut corrupted = TINY_CAR.to_vec();
    // Flip a byte in the final block's data. The block lengths and CIDs are
    // untouched, so iroh-car still frames it — but the bytes no longer hash to
    // their CID.
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xFF;

    let err = DriverBuilder::new()
        .load_car(corrupted.as_slice())
        .await
        .expect_err("a corrupted block must be rejected");

    assert!(
        matches!(err, LoadError::BadCid(_)),
        "expected BadCid, got: {err:?}"
    );
}
