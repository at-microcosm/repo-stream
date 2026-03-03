//! Consume a CAR from an AsyncRead, producing an ordered stream of records

use crate::link::{NodeThing, ObjectLink, ThingKind};
use crate::{
    Bytes, HashMap, Rkey, Step,
    block::{MaybeProcessedBlock, noop},
    disk::{DiskError, DiskStore},
    mst::MstNode,
    walk::{MstError, Output},
};
use cid::Cid;
use iroh_car::CarReader;
use std::convert::Infallible;
use tokio::{io::AsyncRead, sync::mpsc};

use crate::mst::Commit;
use crate::walk::{WalkError, Walker};
use thiserror::Error;

/// An in-order chunk of Rkey + CID + (processed) Block
pub type BlockChunk = Vec<Output>;

/// Errors that can occur while loading a CAR into memory
#[derive(Debug, Error)]
pub enum LoadError<R: AsyncRead + Unpin> {
    #[error("failed reading CAR: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("failed to decode cbor: {0}")]
    BadBlock(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("missing commit")]
    MissingCommit,
    #[error("missing mst root node")]
    MissingRoot,
    #[error("failed to walk mst: {0}")]
    WalkError(#[from] WalkError),
    /// The memory limit was reached before all blocks were loaded.
    ///
    /// The partial state is returned so the caller can decide what to do
    /// (e.g. resume with disk storage via `PartialCar::finish_loading`).
    #[error("partially loaded car")]
    MemoryLimitReached(PartialCar<R>),
}


/// A partially memory-loaded CAR file that hit the memory limit mid-stream.
///
/// Can be resumed with disk storage via `finish_loading`, or discarded.
#[derive(Debug)]
pub struct PartialCar<R: AsyncRead + Unpin> {
    pub(crate) car: CarReader<R>,
    pub(crate) root: Cid,
    pub(crate) process: fn(Bytes) -> Bytes,
    pub(crate) max_size: usize,
    pub(crate) blocks: HashMap<ObjectLink, MaybeProcessedBlock>,
    /// The commit block, if it was seen before the memory limit was reached
    pub commit: Option<Commit>,
}

/// Builder-style driver setup
#[derive(Debug, Clone)]
pub struct DriverBuilder {
    pub mem_limit_mb: usize,
    pub block_processor: fn(Bytes) -> Bytes,
}

impl Default for DriverBuilder {
    fn default() -> Self {
        Self {
            mem_limit_mb: 10,
            block_processor: noop,
        }
    }
}

impl DriverBuilder {
    /// Begin configuring the driver with defaults
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the in-memory size limit, in MiB
    ///
    /// Default: 10 MiB
    pub fn with_mem_limit_mb(mut self, new_limit: usize) -> Self {
        self.mem_limit_mb = new_limit;
        self
    }

    /// Set the block processor
    ///
    /// Default: noop, raw blocks will be emitted
    pub fn with_block_processor(mut self, new_processor: fn(Bytes) -> Bytes) -> Self {
        self.block_processor = new_processor;
        self
    }

    /// Load an atproto repository CAR into memory.
    ///
    /// Returns a `MemCar` ready for walking. If the blocks exceed the memory
    /// limit, returns `Err(LoadError::MemoryLimitReached(partial))` containing
    /// the partial state, which can be resumed with disk storage.
    pub async fn load_car<R: AsyncRead + Unpin>(
        &self,
        reader: R,
    ) -> Result<MemCar, LoadError<R>> {
        load_car(reader, self.block_processor, self.mem_limit_mb).await
    }
}

async fn load_car<R: AsyncRead + Unpin>(
    reader: R,
    process: fn(Bytes) -> Bytes,
    mem_limit_mb: usize,
) -> Result<MemCar, LoadError<R>> {
    let mut block_count = 0;

    let max_size = mem_limit_mb * 2_usize.pow(20);
    let mut mem_blocks = HashMap::new();

    let mut car = CarReader::new(reader).await?;

    let roots = car.header().roots();
    assert_eq!(roots.len(), 1);

    let root = *roots.first().ok_or(LoadError::MissingRoot)?;
    log::debug!("root: {root:?}");

    let mut commit = None;

    let mut mem_size = 0;
    while let Some((cid, data)) = car.next_block().await? {
        block_count += 1;
        // The root commit block is handled separately — never passed to the processor
        if cid == root {
            let c: Commit = serde_ipld_dagcbor::from_slice(&data)?;
            commit = Some(c);
            continue;
        }

        let maybe_processed = MaybeProcessedBlock::maybe(process, data);

        mem_size += maybe_processed.len();
        mem_blocks.insert(cid.into(), maybe_processed);
        if mem_size >= max_size {
            log::debug!("blocks loaded before memory limit: {block_count}");
            return Err(LoadError::MemoryLimitReached(PartialCar {
                car,
                root,
                process,
                max_size,
                blocks: mem_blocks,
                commit,
            }));
        }
    }

    log::debug!("blocks: {block_count}");

    let commit = commit.ok_or(LoadError::MissingCommit)?;

    let root_node: MstNode = match mem_blocks
        .get(&commit.data)
        .ok_or(LoadError::MissingCommit)?
    {
        MaybeProcessedBlock::Processed(_) => Err(WalkError::BadCommitFingerprint)?,
        MaybeProcessedBlock::Raw(bytes) => serde_ipld_dagcbor::from_slice(bytes)?,
    };
    let mut walker = Walker::new(root_node);

    let prev_rkey = walker.step_to_edge(&mem_blocks)?;

    Ok(MemCar {
        commit,
        prev_rkey,
        blocks: mem_blocks,
        walker,
        process,
        next_missing: None,
    })
}

/// A fully loaded in-memory CAR file, ready for MST walking.
#[derive(Debug)]
pub struct MemCar {
    pub commit: Commit,
    /// For CAR slices: the rkey of the last record before this slice's leading edge.
    /// `None` if this slice (or full CAR) starts from the leftmost record in the tree.
    pub prev_rkey: Option<Rkey>,
    pub blocks: HashMap<ObjectLink, MaybeProcessedBlock>,
    walker: Walker,
    process: fn(Bytes) -> Bytes,
    next_missing: Option<NodeThing>,
}

impl MemCar {

    /// Seek forward to the first record at or after `target`.
    ///
    /// Uses the MST structure to skip entire subtrees efficiently.
    /// After this returns, the next `next_chunk` call will start at or after `target`.
    pub fn seek(&mut self, target: &str) -> Result<(), WalkError> {
        self.walker.seek(target, &self.blocks)
    }

    /// Get the next record
    pub fn next(&mut self) -> Result<Option<Output>, WalkError> {
        todo!()
    }

    /// Iterate up to `n` records in rkey order.
    ///
    /// Returns `Step::Value(records)` while records remain, then `Step::End(next_rkey)`
    /// where `next_rkey` is the first rkey after the slice (for CAR slices), or `None`.
    pub fn next_chunk(&mut self, n: usize) -> Result<Step<BlockChunk>, WalkError> {
        if let Some(ref mut missing) = self.next_missing {
            while let Step::Value(sparse_out) =
                self.walker.step_sparse(&self.blocks, self.process)?
            {
                if missing.kind == ThingKind::ChildNode {
                    *missing = NodeThing {
                        link: sparse_out.cid.into(),
                        kind: ThingKind::Record(sparse_out.rkey),
                    };
                }
            }
            return Ok(match &missing.kind {
                ThingKind::ChildNode => Step::End(None),
                ThingKind::Record(rkey) => Step::End(Some(rkey.clone())),
            });
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.walker.step(&self.blocks, self.process) {
                Ok(Step::Value(record)) => out.push(record),
                Ok(Step::End(None)) => break,
                Ok(Step::End(_)) => unreachable!(),
                Err(WalkError::MissingBlock(missing)) => {
                    self.next_missing = Some(*missing);
                    return Ok(Step::Value(out)); // may be empty
                }
                Err(other) => return Err(other),
            }
        }
        if out.is_empty() {
            Ok(Step::End(None))
        } else {
            Ok(Step::Value(out))
        }
    }
}

// ---------------------------------------------------------------------------
// Disk path (kept for future wiring, not yet part of the primary API)
// ---------------------------------------------------------------------------

/// Errors that can happen while consuming blocks via the disk path
#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    #[error("Error from iroh_car: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("Failed to decode commit block: {0}")]
    BadBlock(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("The Commit block reference by the root was not found")]
    MissingCommit,
    #[error("Failed to walk the mst tree: {0}")]
    WalkError(#[from] WalkError),
    #[error("CAR file had no roots")]
    MissingRoot,
    #[error("Storage error")]
    StorageError(#[from] DiskError),
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

impl<R: AsyncRead + Unpin> PartialCar<R> {
    pub async fn finish_loading(
        mut self,
        mut store: DiskStore,
    ) -> Result<(Commit, Option<Rkey>, DiskDriver), DriveError> {
        store = tokio::task::spawn(async move {
            let kvs = self
                .blocks
                .into_iter()
                .map(|(k, v)| (k.to_bytes(), v.into_bytes()));

            store.put_many(kvs)?;
            Ok::<_, DriveError>(store)
        })
        .await??;

        let (tx, mut rx) = mpsc::channel::<Vec<(ObjectLink, MaybeProcessedBlock)>>(1);

        let store_worker = tokio::task::spawn_blocking(move || {
            while let Some(chunk) = rx.blocking_recv() {
                let kvs = chunk
                    .into_iter()
                    .map(|(k, v)| (k.to_bytes(), v.into_bytes()));
                store.put_many(kvs)?;
            }
            Ok::<_, DriveError>(store)
        });

        log::debug!("dumping the rest of the stream...");
        loop {
            let mut mem_size = 0;
            let mut chunk = vec![];
            loop {
                let Some((cid, data)) = self.car.next_block().await? else {
                    break;
                };
                if cid == self.root {
                    let c: Commit = serde_ipld_dagcbor::from_slice(&data)?;
                    self.commit = Some(c);
                    continue;
                }

                let link = cid.into();
                let data = Bytes::from(data);

                let maybe_processed = MaybeProcessedBlock::maybe(self.process, data);
                mem_size += maybe_processed.len();
                chunk.push((link, maybe_processed));
                if mem_size >= (self.max_size / 2) {
                    break;
                }
            }
            if chunk.is_empty() {
                break;
            }
            tx.send(chunk)
                .await
                .map_err(|_| DriveError::ChannelSendError)?;
        }
        drop(tx);
        log::debug!("done. waiting for worker to finish...");

        store = store_worker.await??;

        log::debug!("worker finished.");

        let commit = self.commit.ok_or(DriveError::MissingCommit)?;

        let db_bytes = store
            .get(&commit.data.to_bytes())
            .map_err(|e| DriveError::StorageError(DiskError::DbError(e)))?
            .ok_or(DriveError::MissingCommit)?;

        let node: MstNode = match MaybeProcessedBlock::from_bytes(db_bytes.to_vec()) {
            MaybeProcessedBlock::Processed(_) => Err(WalkError::BadCommitFingerprint)?,
            MaybeProcessedBlock::Raw(bytes) => serde_ipld_dagcbor::from_slice(&bytes)?,
        };
        let walker = Walker::new(node);

        Ok((
            commit,
            None,
            DiskDriver {
                process: self.process,
                state: Some(BigState { store, walker }),
            },
        ))
    }
}

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
        process: noop,
        state: None,
    }
}

impl DiskDriver {
    /// Walk the MST returning up to `n` rkey + record pairs
    ///
    /// ```no_run
    /// # use repo_stream::{drive::{DiskDriver, DriveError, _get_fake_disk_driver}, Step, noop};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DriveError> {
    /// # let mut disk_driver = _get_fake_disk_driver();
    /// while let Step::Value(outputs) = disk_driver.next_chunk(256).await? {
    ///     for output in outputs {
    ///         println!("{}: size={}", output.rkey, output.data.len());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_chunk(&mut self, n: usize) -> Result<Step<Vec<Output>>, DriveError> {
        let process = self.process;

        let mut state = self.state.take().expect("DiskDriver must have Some(state)");

        let (state, res) =
            tokio::task::spawn_blocking(move || -> (BigState, Result<BlockChunk, DriveError>) {
                let mut out = Vec::with_capacity(n);

                for _ in 0..n {
                    let step = match state.walker.disk_step(&state.store, process) {
                        Ok(s) => s,
                        Err(e) => {
                            return (state, Err(e.into()));
                        }
                    };
                    let Step::Value(output) = step else {
                        break;
                    };
                    out.push(output);
                }

                (state, Ok::<_, DriveError>(out))
            })
            .await?;

        self.state = Some(state);

        let out = res?;

        if out.is_empty() {
            Ok(Step::End(None))
        } else {
            Ok(Step::Value(out))
        }
    }

    fn read_tx_blocking(
        &mut self,
        n: usize,
        tx: mpsc::Sender<Result<Step<BlockChunk>, DriveError>>,
    ) -> Result<(), mpsc::error::SendError<Result<Step<BlockChunk>, DriveError>>> {
        let BigState { store, walker } = self.state.as_mut().expect("valid state");

        loop {
            let mut out: BlockChunk = Vec::with_capacity(n);

            for _ in 0..n {
                let step = match walker.disk_step(store, self.process) {
                    Ok(s) => s,
                    Err(e) => return tx.blocking_send(Err(e.into())),
                };

                let Step::Value(output) = step else {
                    break;
                };
                out.push(output);
            }

            if out.is_empty() {
                break;
            }
            tx.blocking_send(Ok(Step::Value(out)))?;
        }

        Ok(())
    }

    /// Spawn the disk reading task into a tokio blocking thread
    ///
    /// ```no_run
    /// # use repo_stream::{drive::{DiskDriver, DriveError, _get_fake_disk_driver}, Step, noop};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DriveError> {
    /// # let mut disk_driver = _get_fake_disk_driver();
    /// let (mut rx, join) = disk_driver.to_channel(512);
    /// while let Some(recvd) = rx.recv().await {
    ///     let outputs = recvd?;
    ///     let Step::Value(outputs) = outputs else { break; };
    ///     for output in outputs {
    ///         println!("{}: size={}", output.rkey, output.data.len());
    ///     }
    ///
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_channel(
        mut self,
        n: usize,
    ) -> (
        mpsc::Receiver<Result<Step<BlockChunk>, DriveError>>,
        tokio::task::JoinHandle<Self>,
    ) {
        let (tx, rx) = mpsc::channel::<Result<Step<BlockChunk>, DriveError>>(1);

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
