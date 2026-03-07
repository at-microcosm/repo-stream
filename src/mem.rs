//! Load a CAR file into memory and walk its MST

use crate::{
    Bytes, HashMap, RepoPath,
    disk::{DiskDriver, DiskError, DiskStore, DriveError, make_disk_driver},
    mst::{Commit, ObjectLink},
    walk::{MaybeProcessedBlock, Output, WalkError, WalkItem, Walker},
};
use cid::Cid;
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
    let root = *roots.first().ok_or(LoadError::MissingRoot)?;
    if roots.len() > 1 {
        log::debug!("CAR has {} roots; ignoring all but the first", roots.len());
    }
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

    let (root_node, root_bytes) = match mem_blocks
        .get(&commit.data)
        .ok_or(LoadError::MissingCommit)?
    {
        MaybeProcessedBlock::Processed(_) => Err(WalkError::BadCommitFingerprint)?,
        MaybeProcessedBlock::Raw(bytes) => (serde_ipld_dagcbor::from_slice(bytes)?, bytes.clone()),
    };
    let root_cid: Cid = commit.data.clone().into();

    Ok(MemCar {
        commit,
        prev_key: None,
        blocks: mem_blocks,
        walker: Walker::new(root_node, root_cid, root_bytes),
        process,
    })
}

/// A fully loaded in-memory CAR file, ready for MST walking.
#[derive(Debug)]
pub struct MemCar {
    pub commit: Commit,
    /// For CAR slices: the key of the last record before this slice's leading edge.
    /// `None` if this slice (or full CAR) starts from the leftmost record in the tree.
    /// Not set automatically — callers may derive it from leading `MissingRecord` items.
    pub prev_key: Option<RepoPath>,
    pub(crate) blocks: HashMap<ObjectLink, MaybeProcessedBlock>,
    walker: Walker,
    process: fn(Bytes) -> Bytes,
}

impl MemCar {
    /// Seek forward to the first record at or after `target`.
    ///
    /// Uses the MST structure to skip entire subtrees efficiently.
    /// After this returns, the next call to `next*` will start at or after `target`.
    pub fn seek(&mut self, target: &str) -> Result<(), WalkError> {
        self.walker.seek(target, &self.blocks)
    }

    /// Get the next item from the walk.
    ///
    /// Returns all `WalkItem` variants as-is, including `MissingRecord` and
    /// `MissingSubtree` for sparse trees and CAR slices. Returns `Ok(None)`
    /// when the walk is complete.
    ///
    /// TODO: make this an implementation of Iterator
    pub fn next(&mut self) -> Result<Option<WalkItem>, WalkError> {
        self.walker.step(&self.blocks, self.process)
    }

    /// Collect up to `n` walk items.
    ///
    /// Like `next`, passes through `MissingRecord` and `MissingSubtree` items.
    /// Returns `Ok(None)` when the walk is complete.
    pub fn next_chunk(&mut self, n: usize) -> Result<Option<Vec<WalkItem>>, WalkError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.walker.step(&self.blocks, self.process)? {
                Some(item) => out.push(item),
                None => break,
            }
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    /// Get the next present record, erroring if any block is absent.
    ///
    /// Returns `Ok(None)` when the walk is complete. Returns
    /// `Err(WalkError::MissingBlock)` if a record block is absent, or
    /// `Err(WalkError::MissingNode)` if an MST node block is absent.
    pub fn next_strict(&mut self) -> Result<Option<Output>, WalkError> {
        match self.walker.step(&self.blocks, self.process)? {
            None => Ok(None),
            Some(WalkItem::Record(out)) => Ok(Some(out)),
            Some(WalkItem::MissingRecord { key, cid }) => Err(WalkError::MissingBlock {
                key,
                cid: Box::new(cid),
            }),
            Some(WalkItem::MissingSubtree { cid }) => {
                Err(WalkError::MissingNode { cid: Box::new(cid) })
            }
            Some(WalkItem::Node { .. }) => unreachable!("step() never emits Node items"),
        }
    }

    /// Collect up to `n` present records, erroring if any block is absent.
    ///
    /// Returns `Ok(None)` when the walk is complete. Returns
    /// `Err(WalkError::MissingBlock)` if a record block is absent, or
    /// `Err(WalkError::MissingNode)` if an MST node block is absent.
    pub fn next_chunk_strict(&mut self, n: usize) -> Result<Option<Vec<Output>>, WalkError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.walker.step(&self.blocks, self.process)? {
                None => break,
                Some(WalkItem::Record(record)) => out.push(record),
                Some(WalkItem::MissingRecord { key, cid }) => {
                    return Err(WalkError::MissingBlock {
                        key,
                        cid: Box::new(cid),
                    });
                }
                Some(WalkItem::MissingSubtree { cid }) => {
                    return Err(WalkError::MissingNode { cid: Box::new(cid) });
                }
                Some(WalkItem::Node { .. }) => unreachable!("step() never emits Node items"),
            }
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    /// Walk the MST emitting records, missing items, **and** MST node blocks.
    ///
    /// Like [`next`] but also yields `WalkItem::Node` for every node descended
    /// into (root first, then children in traversal order). Useful for collecting
    /// or counting the raw node blocks alongside records.
    ///
    /// Note: node bytes are cloned on each descent — see [`Walker::step_with_nodes`].
    pub fn next_with_nodes(&mut self) -> Result<Option<WalkItem>, WalkError> {
        self.walker.step_with_nodes(&self.blocks, self.process)
    }

    /// Get the next key and CID from the walk, without fetching record blocks.
    ///
    /// Record CIDs come directly from MST node entries — record blocks are never
    /// looked up. MST node blocks are still fetched to traverse the tree.
    ///
    /// Returns `Ok(None)` when the walk is complete. Returns
    /// `Err(WalkError::MissingNode)` if a child MST node block is absent.
    pub fn next_keys(&mut self) -> Result<Option<(RepoPath, Cid)>, WalkError> {
        self.walker.step_keys(&self.blocks)
    }

    /// Collect up to `n` key+CID pairs, without fetching record blocks.
    ///
    /// Like [`next_keys`] but collects up to `n` pairs in one call.
    ///
    /// Returns `Ok(None)` when the walk is complete. Returns
    /// `Err(WalkError::MissingNode)` if a child MST node block is absent.
    pub fn next_chunk_keys(&mut self, n: usize) -> Result<Option<Vec<(RepoPath, Cid)>>, WalkError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.walker.step_keys(&self.blocks)? {
                Some(pair) => out.push(pair),
                None => break,
            }
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    /// Collect up to `n` items (records, missing items, and node blocks).
    ///
    /// Like [`next_chunk`] but also includes `WalkItem::Node`. The chunk
    /// size counts all item types, so a chunk of 256 may contain fewer records
    /// than a [`next_chunk`] call of 256.
    pub fn next_chunk_with_nodes(&mut self, n: usize) -> Result<Option<Vec<WalkItem>>, WalkError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.walker.step_with_nodes(&self.blocks, self.process)? {
                Some(item) => out.push(item),
                None => break,
            }
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
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

        let root_cid: Cid = commit.data.clone().into();
        let (node, root_bytes) = match MaybeProcessedBlock::from_bytes(db_bytes.to_vec()) {
            MaybeProcessedBlock::Processed(_) => Err(WalkError::BadCommitFingerprint)?,
            MaybeProcessedBlock::Raw(bytes) => (serde_ipld_dagcbor::from_slice(&bytes)?, bytes),
        };
        let walker = Walker::new(node, root_cid, root_bytes);

        Ok((commit, None, make_disk_driver(store, walker, self.process)))
    }
}

// ---------------------------------------------------------------------------
// jacquard feature: construct a MemCar from a pre-parsed ParsedCar
// ---------------------------------------------------------------------------

/// Errors from [`DriverBuilder::load_jacquard_parsed_car`]
#[cfg(feature = "jacquard")]
#[derive(Debug, thiserror::Error)]
pub enum JacquardLoadError {
    #[error("failed to decode cbor: {0}")]
    BadBlock(#[from] serde_ipld_dagcbor::DecodeError<std::convert::Infallible>),
    #[error("missing commit")]
    MissingCommit,
    #[error("failed to walk mst: {0}")]
    WalkError(#[from] WalkError),
}

#[cfg(feature = "jacquard")]
impl DriverBuilder {
    /// Construct a [`MemCar`] from a pre-parsed [`jacquard_repo::car::reader::ParsedCar`].
    ///
    /// Synchronous alternative to [`load_car`] for callers that already hold a
    /// `ParsedCar` from the jacquard ecosystem. The block processor from
    /// [`with_block_processor`] is applied; the memory limit is ignored since all
    /// blocks are already in memory.
    pub fn load_jacquard_parsed_car(
        &self,
        parsed: jacquard_repo::car::reader::ParsedCar,
    ) -> Result<MemCar, JacquardLoadError> {
        use crate::mst::ObjectLink;

        let process = self.block_processor;
        let root = parsed.root;

        // Decode the commit block at the root CID.
        let commit_bytes = parsed
            .blocks
            .get(&root)
            .ok_or(JacquardLoadError::MissingCommit)?
            .as_ref();
        let commit: Commit = serde_ipld_dagcbor::from_slice(commit_bytes)?;

        // Build the block map from all non-commit blocks.
        let mut blocks = HashMap::new();
        for (cid, data) in parsed.blocks {
            if cid == root {
                continue;
            }
            let maybe_processed = MaybeProcessedBlock::maybe(process, data.to_vec());
            blocks.insert(ObjectLink::from(cid), maybe_processed);
        }

        // Look up and decode the root MST node.
        let root_cid: Cid = commit.data.clone().into();
        let (root_node, root_bytes) = match blocks
            .get(&commit.data)
            .ok_or(JacquardLoadError::MissingCommit)?
        {
            MaybeProcessedBlock::Processed(_) => {
                return Err(WalkError::BadCommitFingerprint.into());
            }
            MaybeProcessedBlock::Raw(bytes) => {
                (serde_ipld_dagcbor::from_slice(bytes)?, bytes.clone())
            }
        };

        Ok(MemCar {
            commit,
            prev_key: None,
            blocks,
            walker: Walker::new(root_node, root_cid, root_bytes),
            process,
        })
    }
}
