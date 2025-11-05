//! Consume an MST block stream, producing an ordered stream of records

use crate::disk::{DiskAccess, DiskStore, DiskWriter, StorageErrorBase};
use ipld_core::cid::Cid;
use iroh_car::CarReader;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::io::AsyncRead;

use crate::mst::{Commit, Node};
use crate::walk::{DiskTrip, Step, Trip, Walker};

/// Errors that can happen while consuming and emitting blocks and records
#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    #[error("Error from iroh_car: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("Failed to decode commit block: {0}")]
    BadBlock(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("The Commit block reference by the root was not found")]
    MissingCommit,
    #[error("The MST block {0} could not be found")]
    MissingBlock(Cid),
    #[error("Failed to walk the mst tree: {0}")]
    Tripped(#[from] Trip),
    #[error("CAR file had no roots")]
    MissingRoot,
}

#[derive(Debug, thiserror::Error)]
pub enum DiskDriveError<E: StorageErrorBase> {
    #[error("Error from iroh_car: {0}")]
    CarReader(#[from] iroh_car::Error),
    #[error("Failed to decode commit block: {0}")]
    BadBlock(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("Storage error")]
    StorageError(#[from] E),
    #[error("The Commit block reference by the root was not found")]
    MissingCommit,
    #[error("The MST block {0} could not be found")]
    MissingBlock(Cid),
    #[error("Encode error: {0}")]
    BincodeEncodeError(#[from] bincode::error::EncodeError),
    #[error("Decode error: {0}")]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
    #[error("disk tripped: {0}")]
    DiskTripped(#[from] DiskTrip<E>),
}

// #[derive(Debug, thiserror::Error)]
// pub enum Boooooo<E: StorageErrorBase> {
//     #[error("disk tripped: {0}")]
//     DiskTripped(#[from] DiskTrip<E>),
//     #[error("dde whatever: {0}")]
//     DiskDriveError(#[from] DiskDriveError<E>),
// }

pub trait Processable: Clone + Serialize + DeserializeOwned {
    /// the additional size taken up (not including its mem::size_of)
    fn get_size(&self) -> usize;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaybeProcessedBlock<T> {
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
    /// There's an alternative here, which would be to kick unprocessable blocks
    /// back to Raw, or maybe even a new RawUnprocessable variant. Then we could
    /// surface the typed error later if needed by trying to reprocess.
    Processed(T),
}

impl<T: Processable> Processable for MaybeProcessedBlock<T> {
    /// TODO this is probably a little broken
    fn get_size(&self) -> usize {
        use std::{cmp::max, mem::size_of};

        // enum is always as big as its biggest member?
        let base_size = max(size_of::<Vec<u8>>(), size_of::<T>());

        let extra = match self {
            Self::Raw(bytes) => bytes.len(),
            Self::Processed(t) => t.get_size(),
        };

        base_size + extra
    }
}

pub enum Vehicle<R: AsyncRead + Unpin, T: Processable> {
    Lil(Commit, MemDriver<T>),
    Big(BigCar<R, T>),
}

pub async fn load_car<R: AsyncRead + Unpin, T: Processable>(
    reader: R,
    process: fn(Vec<u8>) -> T,
    max_size: usize,
) -> Result<Vehicle<R, T>, DriveError> {
    let mut mem_blocks = HashMap::new();

    let mut car = CarReader::new(reader).await?;

    let root = *car
        .header()
        .roots()
        .first()
        .ok_or(DriveError::MissingRoot)?;
    log::debug!("root: {root:?}");

    let mut commit = None;

    // try to load all the blocks into memory
    let mut mem_size = 0;
    while let Some((cid, data)) = car.next_block().await? {
        // the root commit is a Special Third Kind of block that we need to make
        // sure not to optimistically send to the processing function
        if cid == root {
            let c: Commit = serde_ipld_dagcbor::from_slice(&data)?;
            commit = Some(c);
            continue;
        }

        // remaining possible types: node, record, other. optimistically process
        // TODO: get the actual in-memory size to compute disk spill
        let maybe_processed = if Node::could_be(&data) {
            MaybeProcessedBlock::Raw(data)
        } else {
            MaybeProcessedBlock::Processed(process(data))
        };

        // stash (maybe processed) blocks in memory as long as we have room
        mem_size += std::mem::size_of::<Cid>() + maybe_processed.get_size();
        mem_blocks.insert(cid, maybe_processed);
        if mem_size >= max_size {
            return Ok(Vehicle::Big(BigCar {
                car,
                root,
                process,
                max_size,
                mem_blocks,
                commit,
            }));
        }
    }

    // all blocks loaded and we fit in memory! hopefully we found the commit...
    let commit = commit.ok_or(DriveError::MissingCommit)?;

    let walker = Walker::new(commit.data);

    Ok(Vehicle::Lil(
        commit,
        MemDriver {
            blocks: mem_blocks,
            walker,
            process,
        },
    ))
}

/// a paritally memory-loaded car file that needs disk spillover to continue
pub struct BigCar<R: AsyncRead + Unpin, T: Processable> {
    car: CarReader<R>,
    root: Cid,
    process: fn(Vec<u8>) -> T,
    max_size: usize,
    mem_blocks: HashMap<Cid, MaybeProcessedBlock<T>>,
    pub commit: Option<Commit>,
}

fn encode(v: impl Serialize) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
}

pub fn decode<T: Processable>(bytes: &[u8]) -> Result<T, bincode::error::DecodeError> {
    let (t, n) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    assert_eq!(n, bytes.len(), "expected to decode all bytes"); // TODO
    Ok(t)
}

impl<R: AsyncRead + Unpin, T: Processable + Send + 'static> BigCar<R, T> {
    pub async fn finish_loading<S: DiskStore>(
        mut self,
        mut store: S,
    ) -> Result<(Commit, BigCarReady<T, S::Access>), DiskDriveError<S::StorageError>>
    where
        S::Access: Send + 'static,
        S::StorageError: 'static,
    {
        // set up access for real
        let mut access = store.get_access().await?;

        // move access in and back out so we can manage lifetimes
        // dump mem blocks into the store
        access = tokio::task::spawn(async move {
            let mut writer = access.get_writer()?;
            for (k, v) in self.mem_blocks {
                let key_bytes = k.to_bytes();
                let val_bytes = encode(v)?; // TODO
                writer.put(key_bytes, val_bytes)?;
            }
            drop(writer); // cannot outlive access
            Ok::<_, DiskDriveError<S::StorageError>>(access)
        })
        .await
        .unwrap()?;

        // dump the rest to disk (in chunks)
        loop {
            let mut chunk = vec![];
            let mut mem_size = 0;
            loop {
                let Some((cid, data)) = self.car.next_block().await? else {
                    break;
                };
                // we still gotta keep checking for the root since we might not have it
                if cid == self.root {
                    let c: Commit = serde_ipld_dagcbor::from_slice(&data)?;
                    self.commit = Some(c);
                    continue;
                }
                // remaining possible types: node, record, other. optimistically process
                // TODO: get the actual in-memory size to compute disk spill
                let maybe_processed = if Node::could_be(&data) {
                    MaybeProcessedBlock::Raw(data)
                } else {
                    MaybeProcessedBlock::Processed((self.process)(data))
                };
                mem_size += std::mem::size_of::<Cid>() + maybe_processed.get_size();
                chunk.push((cid, maybe_processed));
                if mem_size >= self.max_size {
                    break;
                }
            }
            if chunk.is_empty() {
                break;
            }

            // move access in and back out so we can manage lifetimes
            // dump mem blocks into the store
            access = tokio::task::spawn_blocking(move || {
                let mut writer = access.get_writer()?;
                for (k, v) in chunk {
                    let key_bytes = k.to_bytes();
                    let val_bytes = encode(v)?; // TODO
                    writer.put(key_bytes, val_bytes)?;
                }
                drop(writer); // cannot outlive access
                Ok::<_, DiskDriveError<S::StorageError>>(access)
            })
            .await
            .unwrap()?; // TODO
        }

        let commit = self.commit.ok_or(DiskDriveError::MissingCommit)?;

        let walker = Walker::new(commit.data);

        Ok((
            commit,
            BigCarReady {
                process: self.process,
                access,
                walker,
            },
        ))
    }
}

pub struct BigCarReady<T: Clone, A: DiskAccess> {
    process: fn(Vec<u8>) -> T,
    access: A,
    walker: Walker,
}

impl<T: Processable + Send + 'static, A: DiskAccess + Send + 'static> BigCarReady<T, A> {
    pub async fn next_chunk(
        mut self,
        n: usize,
    ) -> Result<(Self, Option<Vec<(String, T)>>), DiskDriveError<A::StorageError>>
    where
        A::StorageError: Send,
    {
        let mut out = Vec::with_capacity(n);
        (self, out) = tokio::task::spawn_blocking(move || {
            let access = self.access;
            let mut reader = access.get_reader()?;

            for _ in 0..n {
                // walk as far as we can until we run out of blocks or find a record
                match self.walker.disk_step(&mut reader, self.process)? {
                    Step::Missing(cid) => return Err(DiskDriveError::MissingBlock(cid)),
                    Step::Finish => break,
                    Step::Step { rkey, data } => {
                        out.push((rkey, data));
                        continue;
                    }
                };
            }

            drop(reader); // cannot outlive access
            self.access = access;
            Ok::<_, DiskDriveError<A::StorageError>>((self, out))
        })
        .await
        .unwrap()?; // TODO

        if out.is_empty() {
            Ok((self, None))
        } else {
            Ok((self, Some(out)))
        }
    }
}

/// The core driver between the block stream and MST walker
///
/// In the future, PDSs will export CARs in a stream-friendly order that will
/// enable processing them with tiny memory overhead. But that future is not
/// here yet.
///
/// CARs are almost always in a stream-unfriendly order, so I'm reverting the
/// optimistic stream features: we load all block first, then walk the MST.
///
/// This makes things much simpler: we only need to worry about spilling to disk
/// in one place, and we always have a reasonable expecatation about how much
/// work the init function will do. We can drop the CAR reader before walking,
/// so the sync/async boundaries become a little easier to work around.
#[derive(Debug)]
pub struct MemDriver<T: Processable> {
    blocks: HashMap<Cid, MaybeProcessedBlock<T>>,
    walker: Walker,
    process: fn(Vec<u8>) -> T,
}

impl<T: Processable> MemDriver<T> {
    /// Manually step through the record outputs
    pub async fn next_chunk(&mut self, n: usize) -> Result<Option<Vec<(String, T)>>, DriveError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            // walk as far as we can until we run out of blocks or find a record
            match self.walker.step(&mut self.blocks, self.process)? {
                Step::Missing(cid) => return Err(DriveError::MissingBlock(cid)),
                Step::Finish => break,
                Step::Step { rkey, data } => {
                    out.push((rkey, data));
                    continue;
                }
            };
        }

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }
}
