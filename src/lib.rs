/*!
A robust CAR file -> MST walker for atproto

Blocks are buffered in memory up to a configurable limit (default 10 MiB).
If the limit is reached, `load_car` returns `Err(LoadError::MemoryLimitReached(partial))`
containing the partial state, which can later be resumed with disk storage.

A `block_processor` function can be provided for tasks where records are
transformed into a smaller representation to save memory.

Once blocks are loaded, the MST is walked and emitted as chunks of
`(key, cid, processed_block)` records in left-to-right order.

Some MST validations are applied:
- Keys must appear in order
- Keys must be at the correct MST tree layer

`iroh_car` additionally applies a block size limit of `2MiB`.

```
use repo_stream::DriverBuilder;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let reader = include_bytes!("../car-samples/tiny.car").as_slice();
let mut total_size = 0;

let mut mem_car = DriverBuilder::new()
    .with_mem_limit_mb(10)
    .with_block_processor(|rec| rec.len().to_ne_bytes().to_vec())
    .load_car(reader)
    .await?;

while let Some(chunk) = mem_car.next_chunk_strict(256)? {
    for output in chunk {
        let size = usize::from_ne_bytes(output.data.try_into().unwrap());
        total_size += size;
    }
}
println!("sum of size of all records: {total_size}");
# Ok(())
# }
```

If the CAR is too large for memory, handle the `MemoryLimitReached` error:

```no_run
use repo_stream::{DriverBuilder, LoadError};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let reader = tokio::io::stdin();
match DriverBuilder::new()
    .with_mem_limit_mb(10)
    .load_car(reader)
    .await
{
    Ok(mut mem_car) => {
        while let Some(chunk) = mem_car.next_chunk_strict(256)? {
            // process records
        }
    }
    Err(LoadError::MemoryLimitReached(partial)) => {
        // resume with disk storage (see DiskBuilder)
        eprintln!("CAR too large for memory");
    }
    Err(e) => return Err(e.into()),
}
# Ok(())
# }
```

Find more [examples in the repo](https://tangled.org/@microcosm.blue/repo-stream/tree/main/examples).

*/

pub mod disk;
pub mod mem;
pub mod mst;
pub mod slice;
pub mod walk;

pub use disk::{DiskBuilder, DiskDriver, DiskError, DiskStore, DriveError};
#[cfg(feature = "jacquard")]
pub use mem::JacquardLoadError;
pub use mem::{DriverBuilder, LoadCommitError, LoadError, MemCar, PartialCar, PartialCommit};
pub use mst::{CidMismatch, Commit, verify_block_cid};
pub use slice::{SliceError, SliceProof, SliceWalker};
pub use walk::{MstError, Output, WalkError, WalkItem, noop};

pub type Bytes = Vec<u8>;

pub type RepoPath = String;

#[cfg(feature = "hashbrown")]
pub(crate) use hashbrown::HashMap;

#[cfg(not(feature = "hashbrown"))]
pub(crate) use std::collections::HashMap;

#[doc = include_str!("../readme.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
