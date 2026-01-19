extern crate repo_stream;
use repo_stream::{Driver, Output, Step};

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
    expect_rkey: Option<&str>,
    expect_proceeding: Option<&str>,
) {
    let (mut driver, before) = match Driver::load_car(
        bytes,
        |block| block.len().to_ne_bytes().to_vec(),
        10, /* MiB */
    )
    .await
    .unwrap()
    {
        Driver::Memory(_commit, before, mem_driver) => (mem_driver, before),
        Driver::Disk(_) => panic!("too big"),
    };

    assert_eq!(before.as_deref(), expect_preceeding);

    let mut found_records = 0;
    let mut sum = 0;
    let mut found_expected_rkey = false;
    let mut prev_rkey = "".to_string();

    while let Ok(step) = driver.next_chunk(256).await {
        match step {
            Step::Value(records) => {
                for Output { rkey, cid: _, data } in records {
                    found_records += 1;

                    let (int_bytes, _) = data.split_at(size_of::<usize>());
                    let size = usize::from_ne_bytes(int_bytes.try_into().unwrap());

                    sum += size;
                    if Some(rkey.as_str()) == expect_rkey {
                        found_expected_rkey = true;
                    }
                    eprintln!("!!!! {rkey}");
                    assert!(rkey > prev_rkey, "rkeys are streamed in order");
                    prev_rkey = rkey;
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
        assert!(found_expected_rkey);
        assert_eq!(sum, expected_sum);
    } else {
        assert!(!found_expected_rkey);
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
