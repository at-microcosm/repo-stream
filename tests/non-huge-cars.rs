extern crate repo_stream;
use repo_stream::drive::Processable;
use serde::{Deserialize, Serialize};

const TINY_CAR: &'static [u8] = include_bytes!("../car-samples/tiny.car");
const LITTLE_CAR: &'static [u8] = include_bytes!("../car-samples/little.car");
const MIDSIZE_CAR: &'static [u8] = include_bytes!("../car-samples/midsize.car");

#[derive(Clone, Serialize, Deserialize)]
struct S(usize);

impl Processable for S {
    fn get_size(&self) -> usize {
        0 // no additional space taken, just its stack size (newtype is free)
    }
}

async fn test_car(bytes: &[u8], expected_records: usize, expected_sum: usize) {
    let mb = 2_usize.pow(20);

    let mut driver = match repo_stream::drive::load_car(bytes, |block| S(block.len()), 10 * mb)
        .await
        .unwrap()
    {
        repo_stream::drive::Vehicle::Lil(_commit, mem_driver) => mem_driver,
        repo_stream::drive::Vehicle::Big(_) => panic!("too big"),
    };

    let mut records = 0;
    let mut sum = 0;
    let mut found_bsky_profile = false;
    let mut prev_rkey = "".to_string();

    while let Some(pairs) = driver.next_chunk(256).await.unwrap() {
        for (rkey, S(size)) in pairs {
            records += 1;
            sum += size;
            if rkey == "app.bsky.actor.profile/self" {
                found_bsky_profile = true;
            }
            assert!(rkey > prev_rkey, "rkeys are streamed in order");
            prev_rkey = rkey;
        }
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
