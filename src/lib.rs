/*!
A robust CAR file -> MST walker for atproto

Small CARs have their blocks buffered in memory. If a configurable memory limit
is reached while reading blocks, CAR reading is suspended, and can be continued
by providing disk storage to buffer the CAR blocks instead.

A `process` function can be provided for tasks where records are transformed
into a smaller representation, to save memory (and disk) during block reading.

Once blocks are loaded, the MST is walked and emitted as chunks of pairs of
`(rkey, processed_block)` pairs, in order (depth first, left-to-right).

Some MST validations are applied
- Keys must appear in order
- Keys must be at the correct MST tree depth

`iroh_car` additionally applies a block size limit of `2MiB`.

```
use repo_stream::{Driver, DiskStore};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let reader = include_bytes!("../car-samples/tiny.car").as_slice();
let mut total_size = 0;
let process = |rec: Vec<u8>| rec.len(); // block processing: just extract the size
let in_mem_limit = 10; /* MiB */
let db_cache_size = 32; /* MiB */

match Driver::load_car(reader, process, in_mem_limit).await? {

    // if all blocks fit within memory
    Driver::Memory(_commit, mut driver) => {
        while let Some(chunk) = driver.next_chunk(256).await? {
            for (_rkey, size) in chunk {
                total_size += size;
            }
        }
    },

    // if the CAR was too big for in-memory processing
    Driver::Disk(paused) => {
        // set up a disk store we can spill to
        let store = DiskStore::new("some/path.db".into(), db_cache_size).await?;
        // do the spilling, get back a (similar) driver
        let (_commit, mut driver) = paused.finish_loading(store).await?;

        while let Some(chunk) = driver.next_chunk(256).await? {
            for (_rkey, size) in chunk {
                total_size += size;
            }
        }

        // clean up the disk store (drop tables etc)
        driver.reset_store().await?;
    }
};
println!("sum of size of all records: {total_size}");
# Ok(())
# }
```

Disk spilling suspends and returns a `Driver::Disk(paused)` instead of going
ahead and eagerly using disk I/O. This means you have to write a bit more code
to handle both cases, but it allows you to have finer control over resource
usage. For example, you can drive a number of parallel memory CAR workers, and
separately have a different number of disk workers picking up suspended disk
tasks from a queue.

Find more [examples in the repo](https://tangled.org/@microcosm.blue/repo-stream/tree/main/examples).

*/

pub mod mst;
mod walk;

pub mod disk;
pub mod drive;
pub mod process;

pub use disk::{DiskError, DiskStore};
pub use drive::{DriveError, Driver};
pub use mst::Commit;
pub use process::Processable;
