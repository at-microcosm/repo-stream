extern crate repo_stream;
use repo_stream::{Driver, Output, Step};

const RECORD_SLICE: &'static [u8] = include_bytes!("../car-samples/slice-one.car");
const RECORD_NODE_BEFORE: &'static [u8] = include_bytes!("../car-samples/slice-node-before.car");
const RECORD_NODE_AFTER: &'static [u8] = include_bytes!("../car-samples/slice-node-after.car");
// TODO: absense proof (zero records in slice)

async fn test_car_slice(
    bytes: &[u8],
    expected_records: usize,
    expected_sum: usize,
    expect_preceeding: &str,
    expect_rkey: &str,
    expect_proceeding: &str,
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

    assert_eq!(before, Some(expect_preceeding.into()));

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
                    if rkey == expect_rkey {
                        found_expected_rkey = true;
                    }
                    assert!(rkey > prev_rkey, "rkeys are streamed in order");
                    prev_rkey = rkey;
                }
            }
            Step::End(proceeding) => {
                assert_eq!(proceeding, Some(expect_proceeding.into()));
                break;
            }
        }
    }

    assert_eq!(found_records, expected_records);
    assert_eq!(sum, expected_sum);
    assert!(found_expected_rkey);
}

#[tokio::test]
async fn test_record_slice_car() {
    test_car_slice(
        RECORD_SLICE,
        1,
        212,
        "app.bsky.feed.like/3mcfzfbpaml27",
        "app.bsky.feed.like/3mcg72x6bi32z",
        "app.bsky.feed.like/3mcga2o2efq27",
    )
    .await
}

#[tokio::test]
async fn test_record_slice_node_before() {
    test_car_slice(RECORD_NODE_BEFORE, 1, 212, "", "", "").await
}

#[tokio::test]
async fn test_record_slice_node_after() {
    test_car_slice(
        RECORD_NODE_AFTER,
        1,
        212,
        "app.bsky.feed.like/3mbzi6ttskp2c",
        "",
        "",
    )
    .await
}
