use ipld_core::ipld::Ipld;
use tokio::io::AsyncRead;
use iroh_car::CarReader;
use std::collections::HashMap;
use ipld_core::cid::Cid;

use crate::mst::Commit;
use crate::walk::{Walker, Step, Trip};

#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    #[error("Failed to initialize CarReader: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("CAR file requires a root to be present")]
    MissingRoot,
    #[error("Failed to decode commit block: {0}")]
    BadCommit(Box<dyn std::error::Error>),
    #[error("Failed to decode record block: {0}")]
    BadRecord(Box<dyn std::error::Error>),
    #[error("The Commit block reference by the root was not found")]
    MissingCommit,
    #[error("Failed to walk the mst tree: {0}")]
    Tripped(#[from] Trip),
}


pub async fn drive<R: AsyncRead + Unpin>(reader: R) -> Result<(), DriveError> {
    let mut reader = CarReader::new(reader).await?;

    let root = reader
        .header()
        .roots()
        .first()
        .ok_or(DriveError::MissingRoot)?
        .clone();
    log::debug!("root: {root:?}");

    // one day,
    // https://github.com/bluesky-social/proposals/tree/main/0006-sync-iteration#streaming-car-processing

    // block buffers
    let mut blocks: HashMap::<Cid, Vec<u8>> = HashMap::new();

    // stage 1: try to parse out the commit block, buffering other blocks until
    // we find it
    let mut commit = None;
    while let Some((cid, data)) = reader.next_block().await? {
        if cid == root {
            let c: Commit = serde_ipld_dagcbor::from_slice(&data)
                .map_err(|e| DriveError::BadCommit(e.into()))?;
            commit = Some(c);
            break;
        }
        blocks.insert(cid, data);
    };

    // we either broke out or read all the blocks without finding the commit...
    let commit = commit.ok_or(DriveError::MissingCommit)?;

    log::debug!("got the commit: {commit:?}");

    // broke out! found it! yay! and with the commit we should know the tree
    // root, so we can start walking as we go now.
    let mut walker = Walker::new(commit.data);
    let mut n = 0;
    'outer: loop {
        // walk as far as we can, then stream in more blocks
        let mut m = 0;
        loop {
            match walker.walk(&mut blocks)? {
                Step::Rest => {
                    log::trace!("walker is resting, get another block");
                    break;
                }
                Step::Finish => {
                    log::trace!("walker finished");
                    break 'outer;
                }
                Step::Step { rkey, data } => {
                    let rkey = String::from_utf8(rkey);
                    let record: Ipld = serde_ipld_dagcbor::from_slice(&data)
                        .map_err(|e| DriveError::BadRecord(e.into()))?;
                    log::info!("found {rkey:?} => {record:?}");
                }
            }
            m += 1;
            if m > 1000 {
                log::error!("ran out of inner loop time, breaking");
                break 'outer;
            };
        }

        let Some((cid, data)) = reader.next_block().await? else {
            log::warn!("no more data to stream in, but ig walker didn't finish?");
            break;
        };
        blocks.insert(cid, data);

        n += 1;
        if n > 1000 {
            log::error!("ran out of outer loop time, breaking");
            break 'outer;
        };
    }

    log::info!("done! bye!");

    Ok(())
}
