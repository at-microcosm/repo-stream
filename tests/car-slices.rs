extern crate repo_stream;
use repo_stream::{Driver, Output, Step};

const RECORD_SLICE: &'static [u8] = include_bytes!("../car-samples/slice-one.car");

async fn test_car_slice(
    bytes: &[u8],
    expected_records: usize,
    expected_sum: usize,
    expect_rkey: &str,
) {
    let mut driver = match Driver::load_car(
        bytes,
        |block| block.len().to_ne_bytes().to_vec(),
        10, /* MiB */
    )
    .await
    .unwrap()
    {
        Driver::Memory(_commit, _, mem_driver) => mem_driver,
        Driver::Disk(_) => panic!("too big"),
    };

    let mut records = 0;
    let mut sum = 0;
    let mut found_expected_rkey = false;
    let mut prev_rkey = "".to_string();

    while let Step::Value(pairs) = driver.next_chunk(256).await.unwrap() {
        for Output { rkey, cid: _, data } in pairs {
            records += 1;

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

    assert_eq!(records, expected_records);
    assert_eq!(sum, expected_sum);
    assert!(found_expected_rkey);
}

#[tokio::test]
async fn test_record_slice_car() {
    test_car_slice(RECORD_SLICE, 1, 0, "app.bsky.feed.like/3mcg72x6bi32z").await
}
