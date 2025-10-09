# repo-stream

a futures atproto record stream from CAR file

current notes

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
