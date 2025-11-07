//! Fast and robust atproto CAR file processing in rust
//!
//! For now see the [examples](https://tangled.org/@microcosm.blue/repo-stream/tree/main/examples)

mod mst;
mod walk;

pub mod disk;
pub mod drive;
pub mod process;

pub use disk::SqliteStore;
pub use drive::{DriveError, Driver};
pub use process::{Processable, noop};
