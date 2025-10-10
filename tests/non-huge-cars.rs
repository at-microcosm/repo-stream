extern crate repo_stream;
use futures::TryStreamExt;
use iroh_car::CarReader;
use std::convert::Infallible;

const TINY_CAR: &'static [u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &'static [u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &'static [u8] = include_bytes!("../car-samples/midsize.car");

async fn test_car(bytes: &[u8], expected_records: usize, expected_sum: usize) {
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

    let mut records = 0;
    let mut sum = 0;
    let mut found_bsky_profile = false;
    let mut prev_rkey = "".to_string();
    while let Some((rkey, size)) = record_stream.try_next().await.unwrap() {
        records += 1;
        sum += size;
        if rkey == "app.bsky.actor.profile/self" {
            found_bsky_profile = true;
        }
        assert!(rkey > prev_rkey, "rkeys are streamed in order");
        prev_rkey = rkey;
    }
    assert_eq!(records, expected_records);
    assert_eq!(sum, expected_sum);
    assert!(found_bsky_profile);
}

#[tokio::test]
async fn test_tiny_car() {
    test_car(TINY_CAR, 8, 2071).await
}

#[tokio::test]
async fn test_little_car() {
    test_car(LITTLE_CAR, 278, 246960).await
}

#[tokio::test]
async fn test_midsize_car() {
    test_car(MIDSIZE_CAR, 11585, 3741393).await
}
