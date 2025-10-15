use futures::Stream;
use futures::TryStreamExt;
use std::collections::VecDeque;
use std::error::Error;

use crate::disk_walk::{Trip, Walker, RkeyError};
use crate::mst::Commit;

use ipld_core::cid::Cid;
use serde::{Serialize, de::DeserializeOwned};

/// Errors that can happen while consuming and emitting blocks and records
#[derive(Debug, thiserror::Error)]
pub enum DriveError<E: Error> {
    #[error("Failed to initialize CarReader: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("Car block stream error: {0}")]
    CarBlockError(Box<dyn Error>),
    #[error("Failed to decode commit block: {0}")]
    BadCommit(Box<dyn Error>),
    #[error("The Commit block reference by the root was not found")]
    MissingCommit,
    #[error("The MST block {0} could not be found")]
    MissingBlock(Cid),
    #[error("Failed to walk the mst tree: {0}")]
    Tripped(#[from] Trip),
    #[error("whatever: {0}")]
    WalkingProblem(#[from] WalkError),
    #[error("whatever: {0}")]
    Boooooo(String),
    #[error("processing error: {0}")]
    ProcessingError(E),
}

/// Limited subset of errors that can happen while walking
#[derive(Debug, thiserror::Error)]
pub enum WalkError {
    #[error("The MST block {0} could not be found")]
    MissingBlock(Cid),
    #[error("Failed to walk the mst tree: {0}")]
    Tripped(#[from] Trip),
}

#[derive(Debug, thiserror::Error)]
pub enum BlockStoreError {
    #[error("Error from the storage backend: {0}")]
    StorageBackend(Box<dyn Error + Send>),

    #[error(transparent)]
    RkeyError(#[from] RkeyError),

    // this should probably not be up here
    #[error("Failed to join tokio task: {0}")]
    JoinError(tokio::task::JoinError),

    #[error("Could not find block: {0}")]
    MissingBlock(Cid),
}

/// Storage backend for caching large-repo blocks
///
/// Since
pub trait BlockStore<MPB: Serialize + DeserializeOwned> {
    fn put_batch(&self, blocks: Vec<(Cid, MPB)>) -> impl Future<Output = Result<(), BlockStoreError>>; // unwraps for now
    fn walk_batch(
        &self,
        walker: Walker,
        n: usize,
    ) -> impl Future<Output = Result<(Walker, Vec<(String, MPB)>), BlockStoreError>>; // boo string error for now because
}

type CarBlock<E> = Result<(Cid, Vec<u8>), E>;

/// The core driver between the block stream and MST walker
pub struct Vehicle<SE, S, T, BS, P, PE>
where
    SE: Error + 'static,
    S: Stream<Item = CarBlock<SE>>,
    T: Clone + Serialize + DeserializeOwned,
    BS: BlockStore<Vec<u8>>,
    P: Fn(&[u8]) -> Result<T, PE>,
    PE: Error,
{
    #[allow(dead_code)]
    block_stream: Option<S>,
    block_store: BS,
    walker: Walker,
    process: P,
    out_cache: VecDeque<(String, T)>,
}

impl<SE, S, T, BS, P, PE> Vehicle<SE, S, T, BS, P, PE>
where
    SE: Error + 'static,
    S: Stream<Item = CarBlock<SE>> + Unpin + Send,
    T: Clone + Serialize + DeserializeOwned + Send,
    BS: BlockStore<Vec<u8>> + Send,
    P: Fn(&[u8]) -> Result<T, PE> + Send,
    PE: Error,
{
    /// Set up the stream
    ///
    /// This will eagerly consume blocks until the `Commit` object is found.
    /// *Usually* the it's the first block, but there is no guarantee.
    ///
    /// ### Parameters
    ///
    /// `root`: CID of the commit object that is the root of the MST
    ///
    /// `block_stream`: Input stream of raw CAR blocks
    ///
    /// `process`: record-transforming callback:
    ///
    /// For tasks where records can be quickly processed into a *smaller*
    /// useful representation, you can do that eagerly as blocks come in by
    /// passing the processor as a callback here. This can reduce overall
    /// memory usage.
    pub async fn init(
        root: Cid,
        block_stream: S,
        block_store: BS,
        process: P,
    ) -> Result<(Commit, Self), DriveError<PE>> {
        let mut commit = None;

        log::warn!("init: load blocks");

        let mut chunked = block_stream.try_chunks(4096);

        // go ahead and put all blocks in the block store
        while let Some(chunk) = chunked
            .try_next()
            .await
            .map_err(|e| DriveError::CarBlockError(e.into()))?
        {
            let mut to_insert = Vec::with_capacity(chunk.len());
            for (cid, data) in chunk {
                if cid == root {
                    let c: Commit = serde_ipld_dagcbor::from_slice(&data)
                        .map_err(|e| DriveError::BadCommit(e.into()))?;
                    commit = Some(c);
                } else {
                    to_insert.push((cid, data));
                }
            }
            block_store
                .put_batch(to_insert)
                .await
                .map_err(|e| DriveError::Boooooo(format!("boooOOOOO! {e}")))?; // TODO
        }

        log::warn!("init: got commit?");

        // we either broke out or read all the blocks without finding the commit...
        let commit = commit.ok_or(DriveError::MissingCommit)?;

        let walker = Walker::new(commit.data);

        log::warn!("init: wrapping up");

        let me = Self {
            block_stream: None,
            block_store,
            walker,
            process,
            out_cache: VecDeque::new(),
        };
        Ok((commit, me))
    }

    async fn load_chunk(&mut self, n: usize) -> Result<(), DriveError<PE>> {
        let walker = std::mem::take(&mut self.walker);
        let (walker, batch) = self
            .block_store
            .walk_batch(walker, n)
            .await
            .map_err(|e| DriveError::Boooooo(format!("booo! {e}")))?; // TODO
        self.walker = walker;

        let processed = batch
            .into_iter()
            .map(|(k, raw)| (self.process)(&raw).map(|t| (k, t)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(DriveError::ProcessingError)?;

        self.out_cache.extend(processed);
        Ok(())
    }

    /// Get a chunk of records at a time
    ///
    /// the number of returned records may be smaller or larger than requested
    /// (but non-zero), even if it's not the last chunk.
    ///
    /// an empty vec will be returned to signal the end.
    pub async fn next_chunk(&mut self, n: usize) -> Result<Vec<(String, T)>, DriveError<PE>> {
        if self.out_cache.is_empty() {
            self.load_chunk(n).await?;
        }
        Ok(std::mem::take(&mut self.out_cache).into())
    }

    /// Manually step through the record outputs
    pub async fn next_record(&mut self) -> Result<Option<(String, T)>, DriveError<PE>> {
        if self.out_cache.is_empty() {
            self.load_chunk(64).await?; // TODO
        }
        Ok(self.out_cache.pop_front())
    }

    /// Convert to a futures::stream of record outputs
    pub fn stream(self) -> impl Stream<Item = Result<(String, T), DriveError<PE>>> {
        futures::stream::try_unfold(self, |mut this| async move {
            let maybe_record = this.next_record().await?;
            Ok(maybe_record.map(|b| (b, this)))
        })
    }
}
