//! Load a CAR file into memory and walk its MST

use crate::{
    Bytes, HashMap, RepoPath, Step,
    disk::{DiskDriver, DiskError, DiskStore, DriveError, make_disk_driver},
    mst::{Commit, MstNode, ObjectLink},
    walk::{MaybeProcessedBlock, Output, WalkError, WalkItem, Walker},
};
use iroh_car::CarReader;
use std::convert::Infallible;
use thiserror::Error;
use tokio::io::AsyncRead;

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
    ///
    /// boxed because it's big, to avoid making normal load errors heavy
    #[error("partially loaded car")]
    MemoryLimitReached(Box<PartialCar<R>>),
}

/// A partially memory-loaded CAR file that hit the memory limit mid-stream.
///
/// Can be resumed with disk storage via `finish_loading`, or discarded.
#[derive(Debug)]
pub struct PartialCar<R: AsyncRead + Unpin> {
    pub(crate) car: CarReader<R>,
    pub(crate) root: cid::Cid,
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
            block_processor: crate::walk::noop,
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
    pub async fn load_car<R: AsyncRead + Unpin>(&self, reader: R) -> Result<MemCar, LoadError<R>> {
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
            return Err(LoadError::MemoryLimitReached(Box::new(PartialCar {
                car,
                root,
                process,
                max_size,
                blocks: mem_blocks,
                commit,
            })));
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

    Ok(MemCar {
        commit,
        prev_key: None,
        blocks: mem_blocks,
        walker: Walker::new(root_node),
        process,
        trailing_key: None,
    })
}

/// A fully loaded in-memory CAR file, ready for MST walking.
#[derive(Debug)]
pub struct MemCar {
    pub commit: Commit,
    /// For CAR slices: the key of the last record before this slice's leading edge.
    /// `None` if this slice (or full CAR) starts from the leftmost record in the tree.
    pub prev_key: Option<RepoPath>,
    pub blocks: HashMap<ObjectLink, MaybeProcessedBlock>,
    walker: Walker,
    process: fn(Bytes) -> Bytes,
    /// `None` = no gap encountered yet; `Some(k)` = trailing edge determined.
    trailing_key: Option<Option<RepoPath>>,
}

impl MemCar {
    /// Seek forward to the first record at or after `target`.
    ///
    /// Uses the MST structure to skip entire subtrees efficiently.
    /// After this returns, the next `next` or `next_chunk` call will start at or after `target`.
    pub fn seek(&mut self, target: &str) -> Result<(), WalkError> {
        self.walker.seek(target, &self.blocks)
    }

    /// Walk forward past any gaps to determine the trailing edge key.
    fn find_trailing_edge(&mut self) -> Result<Option<RepoPath>, WalkError> {
        let trailing = loop {
            match self.walker.step(&self.blocks, self.process)? {
                Some(WalkItem::Record(r)) => break Some(r.key),
                Some(WalkItem::MissingRecord { key, .. }) => break Some(key),
                Some(WalkItem::MissingSubtree { .. }) => continue,
                None => break None,
            }
        };
        self.trailing_key = Some(trailing.clone());
        Ok(trailing)
    }

    /// Get the next record.
    ///
    /// Returns `Step::Value(output)` for each record in key order, then
    /// `Step::End(None)` at the end of a full CAR, or `Step::End(Some(key))`
    /// for CAR slices where `key` is the first key immediately after the slice.
    ///
    /// TODO: make this an implementation of Iterator
    pub fn next(&mut self) -> Result<Step, WalkError> {
        if let Some(trailing) = &self.trailing_key {
            return Ok(Step::End(trailing.clone()));
        }
        match self.walker.step(&self.blocks, self.process)? {
            Some(WalkItem::Record(out)) => Ok(Step::Value(out)),
            Some(WalkItem::MissingRecord { key, .. }) => {
                self.trailing_key = Some(Some(key.clone()));
                Ok(Step::End(Some(key)))
            }
            Some(WalkItem::MissingSubtree { .. }) => {
                let trailing = self.find_trailing_edge()?;
                Ok(Step::End(trailing))
            }
            None => {
                self.trailing_key = Some(None);
                Ok(Step::End(None))
            }
        }
    }

    /// Iterate up to `n` records in key order.
    ///
    /// Returns `Step::Value(records)` while records remain, then `Step::End(next_key)`
    /// where `next_key` is the first key after the slice (for CAR slices), or `None`.
    pub fn next_chunk(&mut self, n: usize) -> Result<Step<Vec<Output>>, WalkError> {
        if let Some(trailing) = &self.trailing_key {
            return Ok(Step::End(trailing.clone()));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.walker.step(&self.blocks, self.process)? {
                Some(WalkItem::Record(record)) => out.push(record),
                Some(WalkItem::MissingRecord { key, .. }) => {
                    self.trailing_key = Some(Some(key.clone()));
                    return Ok(Step::Value(out)); // may be empty
                }
                Some(WalkItem::MissingSubtree { .. }) => {
                    let trailing = self.find_trailing_edge()?;
                    self.trailing_key = Some(trailing);
                    return Ok(Step::Value(out)); // may be empty
                }
                None => break,
            }
        }
        if out.is_empty() {
            self.trailing_key = Some(None);
            Ok(Step::End(None))
        } else {
            Ok(Step::Value(out))
        }
    }
}

// ---------------------------------------------------------------------------
// Resuming a partial load on disk
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> PartialCar<R> {
    pub async fn finish_loading(
        mut self,
        mut store: DiskStore,
    ) -> Result<(Commit, Option<RepoPath>, DiskDriver), DriveError> {
        use tokio::sync::mpsc;

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

        Ok((commit, None, make_disk_driver(store, walker, self.process)))
    }
}
