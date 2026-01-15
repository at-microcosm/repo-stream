# v0.4.0

_2026-01-15_

- use `Output { rkey, cid, data }` instead of the `(rkey, data)` tuple so that the `Cid` is exposed. this is to make tap-like diffing possible.


# v0.3.1

_2026-01-15_

- bring back the disk driver's `reset` function for disk storage reuse


# v0.3.0

_2026-01-15_

- drop sqlite, pick up fjall v3 for some speeeeeeed (and code simplification and easier build requirements and)
- no more `Processable` trait, process functions are just `Vec<u8> -> Vec<u8>` now (bring your own ser/de). there's a potential small cost here where processors need to now actually go through serialization even for in-memory car walking, but i think zero-copy approaches (eg. rkyv) are low-cost enough
- custom deserialize for MST nodes that does as much depth calculation and rkey validation as - possible in-line. (not clear if it actually made anything faster)
- check MST depth at every node properly (previously it could do some walking before being able to check and included some assumptions)
- check MST for empty leaf nodes (which not allowed)
- shave 0.6 nanoseconds (really) from MST depth calculation (don't ask)
- drop and swap some dependencies: `bincode`, `futures`, `futures-core`, `ipld-core` -> `cid`, `multibase`, `rusqlite` -> `fjall`. and add `hashbrown` bc it benchmarked a bit faster. (we hash on user-controlled CIDs -- is the lower DOS-resistance a risk to worry about?)
