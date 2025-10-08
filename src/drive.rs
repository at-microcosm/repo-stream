use futures::{Stream, TryStreamExt};
use ipld_core::cid::Cid;
use std::collections::HashMap;
use std::error::Error;

use crate::mst::Commit;
use crate::walk::{Step, Trip, Walker};

#[derive(Debug, thiserror::Error)]
pub enum DriveError {
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
    #[error("Failed to walk the mst tree: {0}")]
    Tripped(#[from] Trip),
    #[error("Not finished walking, but no more blocks are available to continue")]
    Dnf,
    #[error("Record Rkey was invalid (not utf-8)")]
    BadRkey,
}

type CarBlock<E> = Result<(Cid, Vec<u8>), E>;

#[derive(Debug)]
pub struct Rkey(pub String);

pub struct Vehicle<E, S: Stream<Item = CarBlock<E>>> {
    block_stream: S,
    blocks: HashMap<Cid, Vec<u8>>,
    walker: Walker,
    walked_out: bool,
}

impl<E: Error + 'static, S: Stream<Item = CarBlock<E>> + Unpin> Vehicle<E, S> {
    pub async fn init(root: &Cid, mut block_stream: S) -> Result<(Commit, Self), DriveError> {
        let mut blocks = HashMap::new();

        let mut commit = None;
        while let Some((block_cid, data)) = block_stream
            .try_next()
            .await
            .map_err(|e| DriveError::CarBlockError(e.into()))?
        {
            if block_cid == *root {
                let c: Commit = serde_ipld_dagcbor::from_slice(&data)
                    .map_err(|e| DriveError::BadCommit(e.into()))?;
                commit = Some(c);
                break; // inner while
            }
            blocks.insert(block_cid, data);
        }

        // we either broke out or read all the blocks without finding the commit...
        let commit = commit.ok_or(DriveError::MissingCommit)?;

        let walker = Walker::new(commit.data);

        let me = Self {
            block_stream,
            blocks,
            walker,
            walked_out: false,
        };
        Ok((commit, me))
    }

    pub async fn next_record(&mut self) -> Result<Option<(Rkey, Vec<u8>)>, DriveError> {
        drive_ahead(self).await
    }

    pub fn stream(self) -> impl Stream<Item = Result<(Rkey, Vec<u8>), DriveError>> {
        futures::stream::try_unfold(self, |mut this| async move {
            let maybe_record = drive_ahead(&mut this).await?;
            Ok(maybe_record.map(|b| (b, this)))
        })
    }
}

async fn drive_ahead<E: Error + 'static, S: Stream<Item = CarBlock<E>> + Unpin>(
    vehicle: &mut Vehicle<E, S>,
) -> Result<Option<(Rkey, Vec<u8>)>, DriveError> {
    loop {
        if vehicle.walked_out {
            // stopped at a rest, try to load more blocks first
            let Some((cid, data)) = vehicle
                .block_stream
                .try_next()
                .await
                .map_err(|e| DriveError::CarBlockError(e.into()))?
            else {
                return Err(DriveError::Dnf);
            };
            vehicle.blocks.insert(cid, data);
            vehicle.walked_out = false;
        }
        // walk as far as we can until we run out of blocks or find a record
        match vehicle.walker.walk(&mut vehicle.blocks)? {
            Step::Rest => {
                log::trace!("walker is resting, get another block");
                vehicle.walked_out = true;
            }
            Step::Finish => {
                log::trace!("walker finished");
                return Ok(None);
            }
            Step::Step { rkey, data } => {
                let rkey = String::from_utf8(rkey).map_err(|_| DriveError::BadRkey)?;
                return Ok(Some((Rkey(rkey), data)));
            }
        }
    }
}
