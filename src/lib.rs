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
use repo_stream::{Driver, DriverBuilder, DiskBuilder};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let reader = include_bytes!("../car-samples/tiny.car").as_slice();
let mut total_size = 0;

match DriverBuilder::new()
    .with_mem_limit_mb(10)
    .with_block_processor(|rec| rec.len()) // block processing: just extract the raw record size
    .load_car(reader)
    .await?
{

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
        let store = DiskBuilder::new().open("some/path.db".into()).await?;
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

pub use disk::{DiskBuilder, DiskError, DiskStore};
pub use drive::{DriveError, Driver, DriverBuilder, NeedDisk};
pub use mst::Commit;
pub use process::Processable;
