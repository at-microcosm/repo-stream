//! Fast and robust atproto CAR file processing in rust
//!
//! For now see the [examples](https://tangled.org/@microcosm.blue/repo-stream/tree/main/examples)

pub mod disk;
pub mod drive;
pub mod mst;
pub mod process;
pub mod walk;

pub use disk::SqliteStore;
pub use drive::{DriveError, Driver};
pub use process::{Processable, noop};
