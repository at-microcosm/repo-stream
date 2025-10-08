# repo-stream

an AsyncRead for atproto MSTs in CAR files

- tries to walk and emit the MST *while streaming in the CAR*
- drops intermediate mst blocks after reading to reduce total memory
- user-provided transform function on record blocks from IPLD

future work:
- flush to disk if needed (sqlite? redb?)  https://bsky.app/profile/divy.zone/post/3m2mf3jqx3k2w
  - either just generally to handle huge CARs, or as a fallback when streaming fails

redb has an in-memory backend, so it would be possible to *always* use it for block caching. user can choose if they want to allow disk or just do memory, and then "spilling" from the cache to disk would be mostly free?
