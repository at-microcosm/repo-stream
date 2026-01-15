# repo-stream

A robust CAR file -> MST walker for atproto

[![Crates.io][crates-badge]](https://crates.io/crates/repo-stream)
[![Documentation][docs-badge]](https://docs.rs/repo-stream)
[![Sponsor][sponsor-badge]](https://github.com/sponsors/uniphil)

[crates-badge]: https://img.shields.io/crates/v/repo-stream.svg
[docs-badge]: https://docs.rs/repo-stream/badge.svg
[sponsor-badge]: https://img.shields.io/badge/at-microcosm-b820f9?labelColor=b820f9&logo=githubsponsors&logoColor=fff

```rust no_run
use repo_stream::{Driver, DriverBuilder, DriveError, DiskBuilder, Output};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // repo-stream takes any AsyncRead as input, like a tokio::fs::File
    let reader = tokio::fs::File::open("repo.car").await?;
    let reader = tokio::io::BufReader::new(reader);

    // example repo workload is simply counting the total record bytes
    let mut total_size = 0;

    match DriverBuilder::new()
        .with_mem_limit_mb(10)
        .with_block_processor( // block processing: just extract the raw record size
          |rec| rec.len().to_ne_bytes().to_vec())
        .load_car(reader)
        .await?
    {

        // if all blocks fit within memory
        Driver::Memory(_commit, mut driver) => {
            while let Some(chunk) = driver.next_chunk(256).await? {
                for Output { rkey: _, cid: _, data } in chunk {
                    let size = usize::from_ne_bytes(data.try_into().unwrap());
                    total_size += size;
                }
            }
        },

        // if the CAR was too big for in-memory processing
        Driver::Disk(paused) => {
            // set up a disk store we can spill to
            let store = DiskBuilder::new().open("some/path.db".into()).await?;
            // do the spilling, get back a (similar) driver
            let (_commit, mut driver) = paused.finish_loading(store).await?;

            while let Some(chunk) = driver.next_chunk(256).await? {
                for Output { rkey: _, cid: _, data } in chunk {
                    let size = usize::from_ne_bytes(data.try_into().unwrap());
                    total_size += size;
                }
            }
        }
    };
    println!("sum of size of all records: {total_size}");
    Ok(())
}
```

more recent todo
- [ ] add a zero-copy rkyv process function example
- [ ] repo car slices
- [ ] lazy-value stream (rkey -> CID diffing for tap-like `#sync` handling)
- [x] get an *emtpy* car for the test suite
- [x] implement a max size on disk limit

some ideas
- [ ] since the disk k/v get/set interface is now so similar to HashMap (blocking, no transactions,), it's probably possible to make a single `Driver` and move the thread stuff from the disk one to generic helper functions. (might create async footguns though)
- [ ] fork iroh-car into a sync version so we can drop tokio as a hard requirement, and offer async via wrapper helper things
- [ ] feature-flag the sha2 crate for hmac-sha256? if someone wanted fewer deps?? then maybe make `hashbrown` also optional vs builtin hashmap?

-----

older stuff (to clean up):


current car processing times (records processed into their length usize, phil's dev machine):

- 450MiB CAR file (huge): `1.3s`
- 128MiB (huge): `350ms`
- 5.0MiB: `6.8ms`
- 279KiB: `160us`
- 3.4KiB: `5.1us`
- empty: `690ns`

it's a little faster with `mimalloc`

```rust
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
```

- 450MiB CAR file: `1.2s` (-8%)
- 128MiB: `300ms` (-14%)
- 5.0MiB: `6.0ms` (-11%)
- 279KiB: `150us` (-7%)
- 3.4KiB: `4.7us` (-8%)
- empty: `670ns` (-4%)

processing CARs requires buffering blocks, so it can consume a lot of memory. repo-stream's in-memory driver has minimal memory overhead, but there are two ways to make it work with less mem (you can do either or both!)

1. spill blocks to disk
2. inline block processing

#### spill blocks to disk

this is a little slower but can greatly reduce the memory used. there's nothing special you need to do for this.


#### inline block processing

if you don't need to store the complete records, you can have repo-stream try to optimistically apply a processing function to the raw blocks as they are streamed in.


#### constrained mem perf comparison

sketchy benchmark but hey. mimalloc is enabled, and the processing spills to disk. inline processing reduces entire records to 8 bytes (usize of the raw record block size):

- 450MiB CAR file: `5.0s` (4.5x slowdown for disk)
- 128MiB: `1.27s` (4.1x slowdown)

fortunately, most CARs in the ATmosphere are very small, so for eg. backfill purposes, the vast majority of inputs will not face this slowdown.


#### running the huge-car benchmark

- to avoid committing it to the repo, you have to pass it in through the env for now.

  ```bash
  HUGE_CAR=~/Downloads/did_plc_redacted.car cargo bench -- huge-car
  ```


todo

- [x] car file test fixtures & validation tests
- [x] make sure we can get the did and signature out for verification
  -> yeah the commit is returned from init
- [x] spec compliance todos
  - [x] assert that keys are ordered and fail if not
  - [x] verify node mst depth from key (possibly pending [interop test fixes](https://github.com/bluesky-social/atproto-interop-tests/issues/5))
- [x] performance todos
  - [x] consume the serialized nodes into a mutable efficient format
    - [x] maybe customize the deserialize impl to do that directly?
  - [x] benchmark and profile
- [x] robustness todos
  - [x] swap the blocks hashmap for a BlockStore trait that can be dumped to redb
    - [x] maybe keep the redb function behind a feature flag?
  - [ ] can we assert a max size of entries for node blocks?
  - [x] figure out why asserting the upper nibble of the fourth byte of a node fails fingerprinting
    -> because it's the upper 3 bytes, not upper 4 byte nibble, oops.
  - [x] max mst depth (to expensive to attack actually)
  - [x] i don't *think* we need a max recursion depth for processing cbor contents since we leave records to the user to decode

newer ideas

- fixing the interleaved mst walk/ block load actually does perform ok: just need the walker to tell the block loader which block we actually need next, so that the block loader can go ahead and load all blocks until that one without checking back with the walker. so i think we're streaming-order ready!


later ideas

- just buffering all the blocks is 2.5x faster than interleaving optimistic walking
  - at least, this is true on huge CARs with the current (stream-unfriendly) pds export behaviour

- transform function is a little tricky because we can't *know* if a block is a record or a node until we actually walk the tree to it (after they're all buffered in memory anyway).
  - still might as well benchmark a test with optimistic block probing+transform on the way in


original ideas:

- tries to walk and emit the MST *while streaming in the CAR*
- drops intermediate mst blocks after reading to reduce total memory
- user-provided transform function on record blocks from IPLD

future work:
- flush to disk if needed (sqlite? redb?)  https://bsky.app/profile/divy.zone/post/3m2mf3jqx3k2w
  - either just generally to handle huge CARs, or as a fallback when streaming fails

redb has an in-memory backend, so it would be possible to *always* use it for block caching. user can choose if they want to allow disk or just do memory, and then "spilling" from the cache to disk would be mostly free?


## license

This work is dual-licensed under MIT and Apache 2.0. You can choose between one of them if you use this work.

`SPDX-License-Identifier: MIT OR Apache-2.0`
