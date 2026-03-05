extern crate repo_stream;
use repo_stream::{DriverBuilder, LoadError, Output, WalkItem};

const RECORD_SLICE: &[u8] = include_bytes!("../car-samples/slice-one.car");
const RECORD_NODE_FIRST_KEY: &[u8] = include_bytes!("../car-samples/slice-node-first-key.car");
const RECORD_NODE_AFTER: &[u8] = include_bytes!("../car-samples/slice-node-after.car");
const RECORD_NODE_ABSENT: &[u8] = include_bytes!("../car-samples/slice-proving-absence.car");

/// Walk a CAR slice and assert on:
/// - `expect_preceding`: the last `MissingRecord` key before any present records
///   (i.e. the key just before the slice's window)
/// - `expected_records`: count of present records
/// - `expected_sum`: sum of record sizes (via processor)
/// - `expect_key`: a specific key that must appear among the present records
/// - `expect_trailing`: the first `MissingRecord` key after the last present record
///   (i.e. the key just after the slice's window)
async fn test_car_slice(
    bytes: &[u8],
    expected_records: usize,
    expected_sum: usize,
    expect_preceding: Option<&str>,
    expect_key: Option<&str>,
    expect_trailing: Option<&str>,
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

    let mut found_records = 0;
    let mut sum = 0;
    let mut found_expected_key = false;
    let mut prev_key = "".to_string();

    // The last MissingRecord key seen before the first present record.
    let mut preceding: Option<String> = None;
    // The first MissingRecord key seen after the last present record.
    let mut trailing: Option<String> = None;
    let mut after_records = false;

    while let Some(items) = mem_car.next_chunk(256).unwrap() {
        for item in items {
            match item {
                WalkItem::Record(Output { key, cid: _, data }) => {
                    after_records = true;
                    trailing = None; // a later MissingRecord replaces this
                    found_records += 1;

                    let (int_bytes, _) = data.split_at(size_of::<usize>());
                    let size = usize::from_ne_bytes(int_bytes.try_into().unwrap());
                    sum += size;

                    if Some(key.as_str()) == expect_key {
                        found_expected_key = true;
                    }
                    assert!(key > prev_key, "keys are streamed in order");
                    prev_key = key;
                }
                WalkItem::MissingRecord { key, .. } => {
                    if !after_records {
                        preceding = Some(key);
                    } else if trailing.is_none() {
                        trailing = Some(key);
                    }
                }
                WalkItem::MissingSubtree { .. } | WalkItem::Node { .. } => {}
            }
        }
    }

    assert_eq!(found_records, expected_records);
    assert_eq!(preceding.as_deref(), expect_preceding);
    assert_eq!(trailing.as_deref(), expect_trailing);

    if expected_records > 0 {
        assert!(found_expected_key);
        assert_eq!(sum, expected_sum);
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
    // proves `app.bsky.feed.like/3lohfzs6qea23` is absent.
    // the included MST nodes contain entries for neighbouring keys whose
    // record blocks are not in this CAR — they surface as MissingRecord items.
    // no present records; the last MissingRecord key seen is the neighbour.
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
