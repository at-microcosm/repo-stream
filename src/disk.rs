/*!
Disk storage and disk-based MST walking.

```no_run
# use repo_stream::{DiskBuilder, DiskError};
# #[tokio::main]
# async fn main() -> Result<(), DiskError> {
let store = DiskBuilder::new()
    .with_cache_size_mb(32)
    .with_max_stored_mb(1024) // errors when >1GiB of processed blocks are inserted
    .open("/some/path.db".into()).await?;
# Ok(())
# }
```
*/

use crate::{
    Bytes,
    mst::ThingKind,
    walk::{MaybeProcessedBlock, MstError, Output, WalkError, WalkItem, Walker},
};
use fjall::{Database, Error as FjallError, Keyspace, KeyspaceCreateOptions};
use std::convert::Infallible;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Disk storage errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    /// A wrapped database error
    #[error(transparent)]
    DbError(#[from] FjallError),
    /// A tokio blocking task failed to join
    #[error("Failed to join a tokio blocking task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    /// The total size of stored blocks exceeded the allowed size
    #[error("Maximum disk size reached")]
    MaxSizeExceeded,
}

// ---------------------------------------------------------------------------
// Disk driver errors
// ---------------------------------------------------------------------------

/// Errors that can happen while consuming blocks via the disk path
#[derive(Debug, Error)]
pub enum DriveError {
    #[error("Error from iroh_car: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("Failed to decode commit block: {0}")]
    BadBlock(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("The Commit block referenced by the root was not found")]
    MissingCommit,
    #[error("Failed to walk the MST: {0}")]
    WalkError(#[from] WalkError),
    #[error("CAR file had no roots")]
    MissingRoot,
    #[error("Storage error: {0}")]
    StorageError(#[from] DiskError),
    #[error("Unexpected missing block: {0:?}")]
    MissingBlock(Box<cid::Cid>),
    #[error("Tried to send on a closed channel")]
    ChannelSendError,
    #[error("Failed to join a task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

impl From<MstError> for DriveError {
    fn from(me: MstError) -> DriveError {
        DriveError::WalkError(WalkError::MstError(me))
    }
}

// ---------------------------------------------------------------------------
// Disk store
// ---------------------------------------------------------------------------

/// Builder-style disk store setup
#[derive(Debug, Clone)]
pub struct DiskBuilder {
    /// Database in-memory cache allowance
    ///
    /// Default: 32 MiB
    pub cache_size_mb: usize,
    /// Database stored block size limit
    ///
    /// Default: 10 GiB
    ///
    /// Note: actual size on disk may be more, but should approximately scale
    /// with this limit
    pub max_stored_mb: usize,
}

impl Default for DiskBuilder {
    fn default() -> Self {
        Self {
            cache_size_mb: 64,
            max_stored_mb: 10 * 1024, // 10 GiB
        }
    }
}

impl DiskBuilder {
    /// Begin configuring the storage with defaults
    pub fn new() -> Self {
        Default::default()
    }
    /// Set the in-memory cache allowance for the database
    ///
    /// Default: 64 MiB
    pub fn with_cache_size_mb(mut self, size: usize) -> Self {
        self.cache_size_mb = size;
        self
    }
    /// Set the approximate stored block size limit
    ///
    /// Default: 10 GiB
    pub fn with_max_stored_mb(mut self, max: usize) -> Self {
        self.max_stored_mb = max;
        self
    }
    /// Open and initialize the actual disk storage
    pub async fn open(&self, path: PathBuf) -> Result<DiskStore, DiskError> {
        DiskStore::new(path, self.cache_size_mb, self.max_stored_mb).await
    }
}

/// On-disk block storage
pub struct DiskStore {
    #[allow(unused)]
    db: Database,
    keyspace: Keyspace,
    max_stored: usize,
    stored: usize,
}

impl DiskStore {
    /// Initialize a new disk store
    pub async fn new(
        path: PathBuf,
        cache_mb: usize,
        max_stored_mb: usize,
    ) -> Result<Self, DiskError> {
        let max_stored = max_stored_mb * 2_usize.pow(20);
        let (db, keyspace) = tokio::task::spawn_blocking(move || {
            let db = Database::builder(path)
                .manual_journal_persist(true)
                .worker_threads(1)
                .cache_size(cache_mb as u64 * 2_u64.pow(20) / 2)
                .temporary(true)
                .open()?;
            let opts = KeyspaceCreateOptions::default()
                .expect_point_read_hits(true)
                .max_memtable_size(16 * 2_u64.pow(20));
            let keyspace = db.keyspace("z", || opts)?;

            Ok::<_, DiskError>((db, keyspace))
        })
        .await??;

        Ok(Self {
            db,
            keyspace,
            max_stored,
            stored: 0,
        })
    }

    pub(crate) fn put_many(
        &mut self,
        kv: impl Iterator<Item = (Vec<u8>, Bytes)>,
    ) -> Result<(), DiskError> {
        let mut batch = self.db.batch();
        for (k, v) in kv {
            self.stored += v.len();
            if self.stored > self.max_stored {
                return Err(DiskError::MaxSizeExceeded);
            }
            batch.insert(&self.keyspace, k, v);
        }
        batch.commit().map_err(DiskError::DbError)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<fjall::Slice>, FjallError> {
        self.keyspace.get(key)
    }

    /// Drop and recreate the kv table
    pub async fn reset(&self) -> Result<(), DiskError> {
        let keyspace = self.keyspace.clone();
        Ok(tokio::task::spawn_blocking(move || keyspace.clear()).await??)
    }
}

// ---------------------------------------------------------------------------
// disk_step on Walker (impl in this module to avoid walk.rs → disk.rs dep)
// ---------------------------------------------------------------------------

impl Walker {
    /// blocking!!!!!
    pub(crate) fn disk_step(
        &mut self,
        blocks: &DiskStore,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<WalkItem>, WalkError> {
        while let Some(thing) = self.next_todo() {
            let Some(block_slice) = blocks.get(&thing.link.to_bytes())? else {
                return Ok(Some(match thing.kind {
                    ThingKind::Record(key) => WalkItem::MissingRecord {
                        key,
                        cid: thing.link.into(),
                    },
                    ThingKind::ChildNode => WalkItem::MissingSubtree {
                        cid: thing.link.into(),
                    },
                }));
            };
            let mpb = MaybeProcessedBlock::from_bytes(block_slice.to_vec());
            if let Some(out) = self.mpb_step(thing, &mpb, &process)? {
                return Ok(Some(WalkItem::Record(out)));
            }
        }
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Disk driver
// ---------------------------------------------------------------------------

struct BigState {
    store: DiskStore,
    walker: Walker,
}

/// MST walker that reads from disk instead of an in-memory hashmap
pub struct DiskDriver {
    process: fn(Bytes) -> Bytes,
    state: Option<BigState>,
}

// for doctests only
#[doc(hidden)]
pub fn _get_fake_disk_driver() -> DiskDriver {
    DiskDriver {
        process: crate::walk::noop,
        state: None,
    }
}

impl DiskDriver {
    /// Walk the MST returning up to `n` key + record pairs.
    ///
    /// Returns `Ok(Some(outputs))` while records remain, `Ok(None)` when done.
    /// Errors if any block is absent (disk path always expects all blocks present).
    ///
    /// ```no_run
    /// # use repo_stream::disk::{DiskDriver, DriveError, _get_fake_disk_driver};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DriveError> {
    /// # let mut disk_driver = _get_fake_disk_driver();
    /// while let Some(outputs) = disk_driver.next_chunk(256).await? {
    ///     for output in outputs {
    ///         println!("{}: size={}", output.key, output.data.len());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_chunk(&mut self, n: usize) -> Result<Option<Vec<Output>>, DriveError> {
        let process = self.process;

        let mut state = self.state.take().expect("DiskDriver must have Some(state)");

        let (state, res) =
            tokio::task::spawn_blocking(move || -> (BigState, Result<Vec<Output>, DriveError>) {
                let mut out = Vec::with_capacity(n);

                for _ in 0..n {
                    match state.walker.disk_step(&state.store, process) {
                        Err(e) => return (state, Err(e.into())),
                        Ok(Some(WalkItem::Record(output))) => out.push(output),
                        Ok(Some(WalkItem::MissingRecord { cid, .. }))
                        | Ok(Some(WalkItem::MissingSubtree { cid })) => {
                            return (state, Err(DriveError::MissingBlock(Box::new(cid))));
                        }
                        Ok(None) => break,
                        Ok(Some(WalkItem::Node { .. })) => {
                            unreachable!("disk_step never emits Node items")
                        }
                    }
                }

                (state, Ok::<_, DriveError>(out))
            })
            .await?;

        self.state = Some(state);

        let out = res?;

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    fn read_tx_blocking(
        &mut self,
        n: usize,
        tx: mpsc::Sender<Result<Vec<Output>, DriveError>>,
    ) -> Result<(), mpsc::error::SendError<Result<Vec<Output>, DriveError>>> {
        let BigState { store, walker } = self.state.as_mut().expect("valid state");

        loop {
            let mut out: Vec<Output> = Vec::with_capacity(n);

            for _ in 0..n {
                match walker.disk_step(store, self.process) {
                    Err(e) => return tx.blocking_send(Err(e.into())),
                    Ok(Some(WalkItem::Record(output))) => out.push(output),
                    Ok(Some(WalkItem::MissingRecord { cid, .. }))
                    | Ok(Some(WalkItem::MissingSubtree { cid })) => {
                        return tx.blocking_send(Err(DriveError::MissingBlock(Box::new(cid))));
                    }
                    Ok(None) => break,
                    Ok(Some(WalkItem::Node { .. })) => {
                        unreachable!("disk_step never emits Node items")
                    }
                }
            }

            if out.is_empty() {
                break;
            }
            tx.blocking_send(Ok(out))?;
        }

        Ok(())
    }

    /// Spawn the disk reading task into a tokio blocking thread.
    ///
    /// The channel sends `Ok(chunk)` for each batch of records. When the walk
    /// is complete the sender is dropped and `rx.recv()` returns `None`.
    ///
    /// ```no_run
    /// # use repo_stream::disk::{DiskDriver, DriveError, _get_fake_disk_driver};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DriveError> {
    /// # let mut disk_driver = _get_fake_disk_driver();
    /// let (mut rx, join) = disk_driver.to_channel(512);
    /// while let Some(chunk) = rx.recv().await {
    ///     for output in chunk? {
    ///         println!("{}: size={}", output.key, output.data.len());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_channel(
        mut self,
        n: usize,
    ) -> (
        mpsc::Receiver<Result<Vec<Output>, DriveError>>,
        tokio::task::JoinHandle<Self>,
    ) {
        let (tx, rx) = mpsc::channel::<Result<Vec<Output>, DriveError>>(1);

        let chan_task = tokio::task::spawn_blocking(move || {
            if let Err(mpsc::error::SendError(_)) = self.read_tx_blocking(n, tx) {
                log::debug!("big car reader exited early due to dropped receiver channel");
            }
            self
        });

        (rx, chan_task)
    }

    /// Reset the disk storage so it can be reused.
    pub async fn reset_store(mut self) -> Result<DiskStore, DriveError> {
        let BigState { store, .. } = self.state.take().expect("valid state");
        store.reset().await?;
        Ok(store)
    }
}

// ---------------------------------------------------------------------------
// PartialCar::finish_loading lives in mem.rs but needs DiskDriver — it's
// imported there from this module.
// ---------------------------------------------------------------------------

/// Build a `DiskDriver` from a walker and store. Used by `PartialCar::finish_loading`.
pub(crate) fn make_disk_driver(
    store: DiskStore,
    walker: Walker,
    process: fn(Bytes) -> Bytes,
) -> DiskDriver {
    DiskDriver {
        process,
        state: Some(BigState { store, walker }),
    }
}
