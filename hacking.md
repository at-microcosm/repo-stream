things are changing a lot right now, but hopefully this file won't get too out of date.

the latest published repo-stream release works well but it turns out there's still a lot left to do


### memory limit

the last release had a kind of singular concept of a "memory limit", after which it would refuse to process in-memory and give you the partial state to finish dealing with using the disk driver.

the idea for sending the state back to the user was that disk resources might be something they'd want to constrain, while allowing high concurrency of in-memory processing (eg., for large network backfills)

one problem is that the high cost of disk spilling leads to high memory limits. queuing partial work, loaded to the max mem limit, can end up occupying a lot of memory! we probably actually need to pull the concept of a "disk worker" back to something like a "high-resource worker":

- lower mem limit for normal in-memory processing
- queue lower-limit partial state for an available high-resource worker
- high resource worker might continue trying to process in-memory to a higher limit before disk spilling

this should keep things a little more under control without giving up higher-memory in-memory performance -- generally everything should behave more predictably hopefully.


### disk spilling

the switch to fjall was a major performance boost compared to sqlite, but it might not be the ultimate best fit for this

- its WAL can't be turned off. we don't need a WAL. WAL writes go at least into the OS page cache, and while they might never hit disk they still use resources. if the page cache needs to evict, (high overall memory utilization, what we are designing for!), then this could suddenly increase IO, slowing down the high-resource worker, increasing contention over disk bandwidth, and using more disk space.
- its memory isn't super well under control. the amount it actually uses is currently higher than what the user configures with repo-stream (an internal repo-stream problem mostly, not fully fjall's problem) -- some of its impressive performance is probably due to this.
- it launches background workers for compaction etc (extra resource usage, maybe some unfair perf vs sqlite comes from here too)
- it opens a lot of files. if we keep fjall, we should make a global database instance and have individual workers create and drop keyspaces in it, instead of opening many fjall dbs and making the user's app hit ulimit.

i'm interested in seeing whether using fjall's LSM-Tree storage engine directly might help address all of these points.

other storage engines have been tested (redb, microsoft's new neat one, candystore, heed, cask) and so far fjall and sqlite have kept the best balance of controllable resource usage and performance. but i'm still interested in new ones to try.

new ones to try:
  - https://github.com/arthurprs/canopydb: B+ tree so not holding my breath but let's see

sekoia has some nice ideas for a custom storage engine for repo-stream: that's what we'll ultimately switch to most likely!


### partial CAR files

this is the big one currently: repo-stream originally assumed it was working with full CAR exports (every MST link present), but that's not the case for CAR slices (from `com.atproto.sync.getRecord`) or firehose commits (`com.atproto.sync.subscribeRepos` contains a spars trees), and it won't be the case in the future for the sync1.1 collection-subset repo export.

my original attempt focused too closely on CAR slices for `getRecord`, making annoying assumptions that limited it. instead we really just need richer APIs. the `getRecord` case and a collection-subset case could both be served by a range iterator, where getRecord would just tighten the bounds to one exact key. more below.


### (de)serialization

there is a custom MST node deserializer right now which tries to parse the node directly into local data structures. it might have been a very small perf win, but annoyingly it means we lost *serialization* functionality.

we could (maybe should?) implement a custom serializer. or we could just go back to the original `derive` impl so we get it back for free.

(i had been thinking that the custom derive would eventually lead to a custom CBOR binary parser specialized for MST nodes -- i really don't see why not since the subset we need to handle is very small. but anyway.)

it turns out there are use-cases for emitting not just records but MST nodes as well from repo-stream: for example, to build a converter to STAR formats.

so we need to at least have a proper `Node` type we can emit, and ideally that thing derives `serde::Serialize`.


### iroh-car

iroh-car is good but annoyingly async. since storage engines in rust are mostly sync, it makes a bit of friction. wrapping its async calls in a blocking executor might be ok but kind of annoying. also most projects will probably wind up using it in an overal async context.

i want to fork iroh-car and refactor it to a sans-io core, with sync/async wrapping interfaces.


### richer apis

the apis kind of go out in a few dimensions

- output MST nodes or not
- output record contents or just keys and CIDs
- chunked APIs or individual
- failure on missing blocks or Optional output values

feels like there should be

low-level:

- iterate all blocks forward, optional everything
- seek: skip to some part of the tree

for now leaving reverse iteration out for reconsideration if a use-case arises

higher-level

- function to iterate all records, expecting them to all be there (output: (key, cid, contents))
- function to iterate over a range of bounds
- function to get a specific key
- function to iterate over a prefix (with validation of proven correct start/stop bounds?)


### MST validity

- maximum number of entries should be 200 (see previous work with Sekoia)
- maximum number entries of a two-level subtree should be 800 or whatever (get real number, again prev work)

we should also try to make a standards push to get those limits explicitly stated in the spec, to avoid hurting interop.


### processor function

TODO: describe
