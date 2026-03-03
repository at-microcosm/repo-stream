extern crate repo_stream;
use repo_stream::{DriverBuilder, LoadError, Output, Step};

const RECORD_SLICE: &'static [u8] = include_bytes!("../car-samples/slice-one.car");
const RECORD_NODE_FIRST_KEY: &'static [u8] =
    include_bytes!("../car-samples/slice-node-first-key.car");
const RECORD_NODE_AFTER: &'static [u8] = include_bytes!("../car-samples/slice-node-after.car");
const RECORD_NODE_ABSENT: &'static [u8] =
    include_bytes!("../car-samples/slice-proving-absence.car");

async fn test_car_slice(
    bytes: &[u8],
    expected_records: usize,
    expected_sum: usize,
    expect_preceeding: Option<&str>,
    expect_key: Option<&str>,
    expect_proceeding: Option<&str>,
) {
    let mut mem_car = match DriverBuilder::new()
        .with_block_processor(|block| block.len().to_ne_bytes().to_vec())
        .load_car(bytes)
        .await
    {
        Ok(mc) => mc,
        Err(LoadError::MemoryLimitReached(_)) => panic!("too big"),
        Err(e) => panic!("{e}"),
    };

    assert_eq!(mem_car.prev_key.as_deref(), expect_preceeding);

    let mut found_records = 0;
    let mut sum = 0;
    let mut found_expected_key = false;
    let mut prev_key = "".to_string();

    loop {
        match mem_car.next_chunk(256).unwrap() {
            Step::Value(records) => {
                for Output { key, cid: _, data } in records {
                    found_records += 1;

                    let (int_bytes, _) = data.split_at(size_of::<usize>());
                    let size = usize::from_ne_bytes(int_bytes.try_into().unwrap());

                    sum += size;
                    if Some(key.as_str()) == expect_key {
                        found_expected_key = true;
                    }
                    eprintln!("!!!! {key}");
                    assert!(key > prev_key, "keys are streamed in order");
                    prev_key = key;
                }
            }
            Step::End(proceeding) => {
                assert_eq!(proceeding.as_deref(), expect_proceeding);
                break;
            }
        }
    }

    assert_eq!(found_records, expected_records);
    if expected_records > 0 {
        assert!(found_expected_key);
        assert_eq!(sum, expected_sum);
    } else {
        assert!(!found_expected_key);
    }
}

#[tokio::test]
async fn test_record_slice_car() {
    test_car_slice(
        RECORD_SLICE,
        1,
        212,
        Some("app.bsky.feed.like/3mcfzfbpaml27"),
        Some("app.bsky.feed.like/3mcg72x6bi32z"),
        Some("app.bsky.feed.like/3mcga2o2efq27"),
    )
    .await
}

#[tokio::test]
async fn test_record_slice_node_first_key() {
    test_car_slice(
        RECORD_NODE_FIRST_KEY,
        1,
        212,
        None,
        Some("app.bsky.feed.like/3lohfzs6qea24"),
        Some("app.bsky.feed.post/3m72vlnelw227"),
    )
    .await
}

#[tokio::test]
async fn test_record_slice_node_after() {
    test_car_slice(
        RECORD_NODE_AFTER,
        1,
        212,
        Some("app.bsky.feed.like/3mbzi6ttskp2c"),
        Some("app.bsky.feed.like/3mcqqwzsc7x26"),
        Some("app.bsky.feed.post/3lbn6of6qxc2a"),
    )
    .await
}

#[tokio::test]
async fn test_record_slice_proving_absence() {
    // missing key is `app.bsky.feed.like/3lohfzs6qea23`
    // NOTE: repo-stream output here isn't enough info for proof
    test_car_slice(
        RECORD_NODE_ABSENT,
        0,
        0,
        Some("app.bsky.feed.post/3m72vlnelw227"),
        None,
        None,
    )
    .await
}
