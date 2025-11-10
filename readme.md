# repo-stream

A robust CAR file -> MST walker for atproto

[![Crates.io][crates-badge]](https://crates.io/crates/repo-stream)
[![Documentation][docs-badge]](https://docs.rs/repo-stream)

[crates-badge]: https://img.shields.io/crates/v/repo-stream.svg
[docs-badge]: https://docs.rs/repo-stream/badge.svg


more recent todo

- [ ] get an *emtpy* car for the test suite
- [x] implement a max size on disk limit


-----

older stuff (to clean up):


current car processing times (records processed into their length usize, phil's dev machine):

- 128MiB CAR file: `347ms`
- 5.0MiB: `6.1ms`
- 279KiB: `139us`
- 3.4KiB: `4.9us`


running the huge-car benchmark

- to avoid committing it to the repo, you have to pass it in through the env for now.

  ```bash
  HUGE_CAR=~/Downloads/did_plc_redacted.car cargo bench -- huge-car
  ```


todo

- [x] car file test fixtures & validation tests
- [x] make sure we can get the did and signature out for verification
  -> yeah the commit is returned from init
- [ ] spec compliance todos
  - [x] assert that keys are ordered and fail if not
  - [x] verify node mst depth from key (possibly pending [interop test fixes](https://github.com/bluesky-social/atproto-interop-tests/issues/5))
- [ ] performance todos
  - [x] consume the serialized nodes into a mutable efficient format
    - [ ] maybe customize the deserialize impl to do that directly?
  - [x] benchmark and profile
- [ ] robustness todos
  - [ ] swap the blocks hashmap for a BlockStore trait that can be dumped to redb
    - [ ] maybe keep the redb function behind a feature flag?
  - [ ] can we assert a max size for node blocks?
  - [x] figure out why asserting the upper nibble of the fourth byte of a node fails fingerprinting
    -> because it's the upper 3 bytes, not upper 4 byte nibble, oops.
  - [ ] max mst depth (there is actually a hard limit but a malicious repo could do anything)
  - [ ] i don't *think* we need a max recursion depth for processing cbor contents since we leave records to the user to decode

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
