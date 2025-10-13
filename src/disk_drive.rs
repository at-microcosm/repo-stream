use futures::Stream;
use futures::TryStreamExt;
use std::error::Error;

use crate::disk_walk::{Step, Trip, Walker};
use crate::mst::Commit;
use crate::mst::Node;

use ipld_core::cid::Cid;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Errors that can happen while consuming and emitting blocks and records
#[derive(Debug, thiserror::Error)]
pub enum DriveError {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaybeProcessedBlock<T: Clone + Serialize> {
    /// A block that's *probably* a Node (but we can't know yet)
    ///
    /// It *can be* a record that suspiciously looks a lot like a node, so we
    /// cannot eagerly turn it into a Node. We only know for sure what it is
    /// when we actually walk down the MST
    Raw(Vec<u8>),
    /// A processed record from a block that was definitely not a Node
    ///
    /// If we _never_ needed this block, then we may have wasted a bit of effort
    /// trying to process it. Oh well.
    ///
    /// Processing has to be fallible because the CAR can have totally-unused
    /// blocks, which can just be garbage. since we're eagerly trying to process
    /// record blocks without knowing for sure that they *are* records, we
    /// discard any definitely-not-nodes that fail processing and keep their
    /// error in the buffer for them. if we later try to retreive them as a
    /// record, then we can surface the error.
    ///
    /// The error type is `String` because we don't really want to put
    /// any constraints like `Serialize` on the error type, and `Error`
    /// at least requires `Display`. It's a compromise.
    ProcessedOk(T),
    Unprocessable(String),
}

pub trait BlockStore<MPB: Serialize + DeserializeOwned> {
    fn put(&self, key: Cid, value: MPB); // unwraps for now
    fn get(&self, key: Cid) -> Option<MPB>;
}

type CarBlock<E> = Result<(Cid, Vec<u8>), E>;

/// The core driver between the block stream and MST walker
pub struct Vehicle<SE, S, T, BS, P, PE>
where
    SE: Error + 'static,
    S: Stream<Item = CarBlock<SE>>,
    T: Clone + Serialize + DeserializeOwned,
    BS: BlockStore<MaybeProcessedBlock<T>>,
    P: Fn(&[u8]) -> Result<T, PE>,
    PE: Error,
{
    #[allow(dead_code)]
    block_stream: S,
    block_store: BS,
    walker: Walker,
    process: P,
}

impl<SE, S, T, BS, P, PE> Vehicle<SE, S, T, BS, P, PE>
where
    SE: Error + 'static,
    S: Stream<Item = CarBlock<SE>> + Unpin,
    T: Clone + Serialize + DeserializeOwned,
    BS: BlockStore<MaybeProcessedBlock<T>>,
    P: Fn(&[u8]) -> Result<T, PE>,
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
        mut block_stream: S,
        block_store: BS,
        process: P,
    ) -> Result<(Commit, Self), DriveError> {
        let mut commit = None;

        log::warn!("init: load blocks");

        // go ahead and put all blocks in the block store
        while let Some((cid, data)) = block_stream
            .try_next()
            .await
            .map_err(|e| DriveError::CarBlockError(e.into()))?
        {
            if cid == root {
                let c: Commit = serde_ipld_dagcbor::from_slice(&data)
                    .map_err(|e| DriveError::BadCommit(e.into()))?;
                commit = Some(c);
            } else {
                block_store.put(
                    cid,
                    if Node::could_be(&data) {
                        MaybeProcessedBlock::Raw(data)
                    } else {
                        match process(&data) {
                            Ok(t) => MaybeProcessedBlock::ProcessedOk(t),
                            Err(e) => MaybeProcessedBlock::Unprocessable(e.to_string()),
                        }
                    },
                );
            }
        }

        log::warn!("init: got commit?");

        // we either broke out or read all the blocks without finding the commit...
        let commit = commit.ok_or(DriveError::MissingCommit)?;

        let walker = Walker::new(commit.data);

        log::warn!("init: wrapping up");

        let me = Self {
            block_stream,
            block_store,
            walker,
            process,
        };
        Ok((commit, me))
    }

    /// Manually step through the record outputs
    pub async fn next_record(&mut self) -> Result<Option<(String, T)>, DriveError> {
        match self.walker.step(&mut self.block_store, &self.process)? {
            Step::Rest(cid) => Err(DriveError::MissingBlock(cid)),
            Step::Finish => Ok(None),
            Step::Step { rkey, data } => Ok(Some((rkey, data))),
        }
    }

    /// Convert to a futures::stream of record outputs
    pub fn stream(self) -> impl Stream<Item = Result<(String, T), DriveError>> {
        futures::stream::try_unfold(self, |mut this| async move {
            let maybe_record = this.next_record().await?;
            Ok(maybe_record.map(|b| (b, this)))
        })
    }
}
