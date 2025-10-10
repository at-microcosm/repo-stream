use futures::{Stream, TryStreamExt};
use ipld_core::cid::Cid;
use std::collections::HashMap;
use std::error::Error;

use crate::mst::{Commit, Node};
use crate::walk::{Step, Trip, Walker};

#[derive(Debug, thiserror::Error)]
pub enum DriveError<E: Error> {
    #[error("Failed to initialize CarReader: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("CAR file requires a root to be present")]
    MissingRoot,
    #[error("Car block stream error: {0}")]
    CarBlockError(Box<dyn Error>),
    #[error("Failed to decode commit block: {0}")]
    BadCommit(Box<dyn Error>),
    #[error("Failed to decode record block: {0}")]
    BadRecord(Box<dyn Error>),
    #[error("The Commit block reference by the root was not found")]
    MissingCommit,
    #[error("The MST block {0} could not be found")]
    MissingBlock(Cid),
    #[error("Failed to walk the mst tree: {0}")]
    Tripped(#[from] Trip<E>),
    #[error("Not finished walking, but no more blocks are available to continue")]
    Dnf,
}

type CarBlock<E> = Result<(Cid, Vec<u8>), E>;

#[derive(Debug)]
pub struct Rkey(pub String);

#[derive(Debug)]
pub enum MaybeProcessedBlock<T, E> {
    /// A block that's *probably* a Node (but we can't know yet)
    ///
    /// It *can be* a record that suspiciously looks a lot like a node, so we
    /// cannot eagerly turn it into a Node. We only know for sure what it is
    /// when we actually walk down the MST
    Raw(Vec<u8>),
    /// A processed record from a block that was definitely not a Node
    ///
    /// Processing has to be fallible because the CAR can have totally-unused
    /// blocks, which can just be garbage. since we're eagerly trying to process
    /// record blocks without knowing for sure that they *are* records, we
    /// discard any definitely-not-nodes that fail processing and keep their
    /// error in the buffer for them. if we later try to retreive them as a
    /// record, then we can surface the error.
    ///
    /// If we _never_ needed this block, then we may have wasted a bit of effort
    /// trying to process it. Oh well.
    ///
    /// It would be nice to store the real error type from the processing
    /// function, but I'm leaving that generics puzzle for later.
    ///
    /// There's an alternative here, which would be to kick unprocessable blocks
    /// back to Raw, or maybe even a new RawUnprocessable variant. Then we could
    /// surface the typed error later if needed by trying to reprocess.
    Processed(Result<T, E>),
}

// TODO: generic error not box dyn nonsense.
pub type ProcRes<T, E> = Result<T, E>;

pub struct Vehicle<SE, S, T, P, PE>
where
    S: Stream<Item = CarBlock<SE>>,
    P: Fn(&[u8]) -> ProcRes<T, PE>,
    PE: Error,
{
    block_stream: S,
    blocks: HashMap<Cid, MaybeProcessedBlock<T, PE>>,
    walker: Walker,
    process: P,
}

impl<SE, S, T: Clone, P, PE> Vehicle<SE, S, T, P, PE>
where
    SE: Error + 'static,
    S: Stream<Item = CarBlock<SE>> + Unpin,
    P: Fn(&[u8]) -> ProcRes<T, PE>,
    PE: Error,
{
    pub async fn init(
        root: Cid,
        mut block_stream: S,
        process: P,
    ) -> Result<(Commit, Self), DriveError<PE>> {
        let mut blocks = HashMap::new();

        let mut commit = None;

        while let Some((cid, data)) = block_stream
            .try_next()
            .await
            .map_err(|e| DriveError::CarBlockError(e.into()))?
        {
            if cid == root {
                let c: Commit = serde_ipld_dagcbor::from_slice(&data)
                    .map_err(|e| DriveError::BadCommit(e.into()))?;
                commit = Some(c);
                break; // inner while
            } else {
                blocks.insert(cid, if Node::could_be(&data) {
                    MaybeProcessedBlock::Raw(data)
                } else {
                    MaybeProcessedBlock::Processed(process(&data))
                });
            }
        }

        // we either broke out or read all the blocks without finding the commit...
        let commit = commit.ok_or(DriveError::MissingCommit)?;

        let walker = Walker::new(commit.data);

        let me = Self {
            block_stream,
            blocks,
            walker,
            process,
        };
        Ok((commit, me))
    }

    async fn drive_until(&mut self, cid_needed: Cid) -> Result<(), DriveError<PE>> {
        while let Some((cid, data)) = self
            .block_stream
            .try_next()
            .await
            .map_err(|e| DriveError::CarBlockError(e.into()))?
        {
            self.blocks.insert(cid, if Node::could_be(&data) {
                MaybeProcessedBlock::Raw(data)
            } else {
                MaybeProcessedBlock::Processed((self.process)(&data))
            });
            if cid == cid_needed {
                return Ok(());
            }
        };

        // if we never found the block
        return Err(DriveError::MissingBlock(cid_needed));
    }

    pub async fn next_record(&mut self) -> Result<Option<(Rkey, T)>, DriveError<PE>> {
        loop {
            // walk as far as we can until we run out of blocks or find a record
            let cid_needed = match self.walker.walk(&mut self.blocks, &self.process)? {
                Step::Rest(cid) => cid,
                Step::Finish => return Ok(None),
                Step::Step { rkey, data } => return Ok(Some((Rkey(rkey), data))),
            };

            // load blocks until we reach that cid
            self.drive_until(cid_needed).await?;
        }
    }

    pub fn stream(self) -> impl Stream<Item = Result<(Rkey, T), DriveError<PE>>> {
        futures::stream::try_unfold(self, |mut this| async move {
            let maybe_record = this.next_record().await?;
            Ok(maybe_record.map(|b| (b, this)))
        })
    }
}
