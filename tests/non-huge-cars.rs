extern crate repo_stream;
use repo_stream::{DriverBuilder, Output, WalkItem};

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

    while let Some(pairs) = mem_car.next_chunk_strict(256).unwrap() {
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

// ---------------------------------------------------------------------------
// next_chunk_keys tests
// ---------------------------------------------------------------------------

async fn count_keys(bytes: &[u8]) -> usize {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .load_car(bytes)
        .await
        .expect("should fit in memory");

    let mut count = 0;
    let mut prev_key = String::new();
    while let Some(pairs) = mem_car.next_chunk_keys(256).unwrap() {
        for (key, _cid) in pairs {
            assert!(key > prev_key, "next_chunk_keys keys must be in order");
            prev_key = key;
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn test_next_chunk_keys_counts() {
    assert_eq!(count_keys(EMPTY_CAR).await, 0);
    assert_eq!(count_keys(TINY_CAR).await, 8);
    assert_eq!(count_keys(LITTLE_CAR).await, 278);
    assert_eq!(count_keys(MIDSIZE_CAR).await, 11585);
}

/// Verify that next_chunk_keys returns the same (key, cid) pairs as next_chunk_strict.
#[tokio::test]
async fn test_next_chunk_keys_agrees_with_strict() {
    let mut mc_strict = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .load_car(TINY_CAR)
        .await
        .unwrap();
    let mut mc_keys = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .load_car(TINY_CAR)
        .await
        .unwrap();

    let mut from_strict = Vec::new();
    while let Some(chunk) = mc_strict.next_chunk_strict(256).unwrap() {
        for output in chunk {
            from_strict.push((output.key, output.cid));
        }
    }

    let mut from_keys = Vec::new();
    while let Some(pairs) = mc_keys.next_chunk_keys(256).unwrap() {
        from_keys.extend(pairs);
    }

    assert_eq!(from_strict, from_keys);
}

// ---------------------------------------------------------------------------
// next_chunk_with_nodes tests
// ---------------------------------------------------------------------------

async fn with_nodes_counts(bytes: &[u8]) -> (usize, usize) {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .load_car(bytes)
        .await
        .expect("should fit in memory");

    let mut records = 0;
    let mut nodes = 0;
    let mut first_item_is_node = None;

    while let Some(items) = mem_car.next_chunk_with_nodes(256).unwrap() {
        for item in &items {
            if first_item_is_node.is_none() {
                first_item_is_node = Some(matches!(item, WalkItem::Node { .. }));
            }
        }
        for item in items {
            match item {
                WalkItem::Record(_) => records += 1,
                WalkItem::Node { .. } => nodes += 1,
                _ => {}
            }
        }
    }
    // The root MST node must always be the first item emitted.
    assert_eq!(
        first_item_is_node,
        Some(true),
        "first item from next_chunk_with_nodes must be a Node"
    );
    (records, nodes)
}

#[tokio::test]
async fn test_next_chunk_with_nodes_counts() {
    // Record counts must match the strict walk.
    let (records, nodes) = with_nodes_counts(EMPTY_CAR).await;
    assert_eq!(records, 0);
    assert_eq!(nodes, 1, "empty MST still has a root node block");

    assert_eq!(with_nodes_counts(TINY_CAR).await.0, 8);
    assert_eq!(with_nodes_counts(LITTLE_CAR).await.0, 278);
    assert_eq!(with_nodes_counts(MIDSIZE_CAR).await.0, 11585);

    // Non-empty CARs have multiple nodes.
    assert!(with_nodes_counts(TINY_CAR).await.1 > 1);
    assert!(with_nodes_counts(LITTLE_CAR).await.1 > 1);
    assert!(with_nodes_counts(MIDSIZE_CAR).await.1 > 1);
}

// ---------------------------------------------------------------------------
// SliceWalker tests on full CARs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_walker() {
    for (bytes, expected) in [(EMPTY_CAR, 0), (TINY_CAR, 8), (LITTLE_CAR, 278)] {
        let mut mem_car = DriverBuilder::new()
            .with_mem_limit_mb(10)
            .load_car(bytes)
            .await
            .unwrap();

        let mut walker = mem_car.full().unwrap();
        let mut count = 0;
        let mut prev_key = String::new();
        while let Some(output) = walker.next().unwrap() {
            assert!(output.key > prev_key, "full() keys must be in order");
            prev_key = output.key;
            count += 1;
        }
        assert_eq!(count, expected);

        let proof = walker.finish().unwrap();
        assert!(
            proof.preceding_key.is_none(),
            "full walk has no preceding key"
        );
        assert!(
            proof.following_key.is_none(),
            "full walk has no following key"
        );
    }
}

#[tokio::test]
async fn test_get_present_key() {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .load_car(TINY_CAR)
        .await
        .unwrap();

    let result = mem_car.get("app.bsky.actor.profile/self").unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().key, "app.bsky.actor.profile/self");
}

#[tokio::test]
async fn test_prefix_walker() {
    let mut mem_car = DriverBuilder::new()
        .with_mem_limit_mb(10)
        .load_car(TINY_CAR)
        .await
        .unwrap();

    let mut walker = mem_car.prefix("app.bsky.actor.profile").unwrap();
    let mut count = 0;
    while let Some(output) = walker.next().unwrap() {
        assert!(
            output.key.starts_with("app.bsky.actor.profile/"),
            "prefix walker must only yield matching keys"
        );
        count += 1;
    }
    assert_eq!(
        count, 1,
        "tiny.car has exactly one app.bsky.actor.profile record"
    );
}
