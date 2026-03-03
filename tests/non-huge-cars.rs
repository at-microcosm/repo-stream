extern crate repo_stream;
use repo_stream::{DriverBuilder, Output, Step};

const EMPTY_CAR: &'static [u8] = include_bytes!("../car-samples/empty.car");
const TINY_CAR: &'static [u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &'static [u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &'static [u8] = include_bytes!("../car-samples/midsize.car");

async fn test_car(
    bytes: &[u8],
    expected_records: usize,
    expected_sum: usize,
    expect_profile: bool,
) {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(bytes)
        .await
        .expect("should fit in memory");

    let mut records = 0;
    let mut sum = 0;
    let mut found_bsky_profile = false;
    let mut prev_key = "".to_string();

    while let Step::Value(pairs) = mem_car.next_chunk(256).unwrap() {
        for Output { key, cid: _, data } in pairs {
            records += 1;

            let (int_bytes, _) = data.split_at(size_of::<usize>());
            let size = usize::from_ne_bytes(int_bytes.try_into().unwrap());

            sum += size;
            if key == "app.bsky.actor.profile/self" {
                found_bsky_profile = true;
            }
            assert!(key > prev_key, "keys are streamed in order");
            prev_key = key;
        }
    }

    assert_eq!(records, expected_records);
    assert_eq!(sum, expected_sum);
    assert_eq!(found_bsky_profile, expect_profile);
}

#[tokio::test]
async fn test_empty_car() {
    test_car(EMPTY_CAR, 0, 0, false).await
}

#[tokio::test]
async fn test_tiny_car() {
    test_car(TINY_CAR, 8, 2071, true).await
}

#[tokio::test]
async fn test_little_car() {
    test_car(LITTLE_CAR, 278, 246960, true).await
}

#[tokio::test]
async fn test_midsize_car() {
    test_car(MIDSIZE_CAR, 11585, 3741393, true).await
}
