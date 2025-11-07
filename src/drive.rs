//! Consume an MST block stream, producing an ordered stream of records

use crate::disk::SqliteStore;
use crate::process::Processable;
use ipld_core::cid::Cid;
use iroh_car::CarReader;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::{io::AsyncRead, sync::mpsc};

use crate::mst::{Commit, Node};
use crate::walk::{Step, WalkError, Walker};

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
    WalkError(#[from] WalkError),
    #[error("CAR file had no roots")]
    MissingRoot,
    #[error("Storage error")]
    StorageError(#[from] rusqlite::Error),
    #[error("Encode error: {0}")]
    BincodeEncodeError(#[from] bincode::error::EncodeError),
    #[error("Tried to send on a closed channel")]
    ChannelSendError, // SendError takes <T> which we don't need
    #[error("Failed to join a task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error(transparent)]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
    #[error("extra bytes remained after decoding")]
    ExtraGarbage,
}

pub type BlockChunk<T> = Vec<(String, T)>;

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

pub enum Driver<R: AsyncRead + Unpin, T: Processable> {
    Lil(Commit, MemDriver<T>),
    Big(BigCar<R, T>),
}

impl<R: AsyncRead + Unpin, T: Processable> Driver<R, T> {
    pub async fn load_car(
        reader: R,
        process: fn(Vec<u8>) -> T,
        max_size: usize,
    ) -> Result<Driver<R, T>, DriveError> {
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
            let maybe_processed = if Node::could_be(&data) {
                MaybeProcessedBlock::Raw(data)
            } else {
                MaybeProcessedBlock::Processed(process(data))
            };

            // stash (maybe processed) blocks in memory as long as we have room
            mem_size += std::mem::size_of::<Cid>() + maybe_processed.get_size();
            mem_blocks.insert(cid, maybe_processed);
            if mem_size >= max_size {
                return Ok(Driver::Big(BigCar {
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

        Ok(Driver::Lil(
            commit,
            MemDriver {
                blocks: mem_blocks,
                walker,
                process,
            },
        ))
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
    pub async fn next_chunk(&mut self, n: usize) -> Result<Option<BlockChunk<T>>, DriveError> {
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

pub fn decode<T: Processable>(bytes: &[u8]) -> Result<T, DecodeError> {
    let (t, n) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    if n != bytes.len() {
        return Err(DecodeError::ExtraGarbage);
    }
    Ok(t)
}

impl<R: AsyncRead + Unpin, T: Processable + Send + 'static> BigCar<R, T> {
    pub async fn finish_loading(
        mut self,
        mut store: SqliteStore,
    ) -> Result<(Commit, BigCarReady<T>), DriveError> {
        // move store in and back out so we can manage lifetimes
        // dump mem blocks into the store
        store = tokio::task::spawn(async move {
            let mut writer = store.get_writer()?;

            let kvs = self
                .mem_blocks
                .into_iter()
                .map(|(k, v)| Ok(encode(v).map(|v| (k.to_bytes(), v))?));

            writer.put_many(kvs)?;
            writer.commit()?;
            Ok::<_, DriveError>(store)
        })
        .await??;

        let (tx, mut rx) = mpsc::channel::<Vec<(Cid, MaybeProcessedBlock<T>)>>(2);

        let store_worker = tokio::task::spawn_blocking(move || {
            let mut writer = store.get_writer()?;

            while let Some(chunk) = rx.blocking_recv() {
                let kvs = chunk
                    .into_iter()
                    .map(|(k, v)| Ok(encode(v).map(|v| (k.to_bytes(), v))?));
                writer.put_many(kvs)?;
            }

            writer.commit()?;
            Ok::<_, DriveError>(store)
        }); // await later

        // dump the rest to disk (in chunks)
        log::debug!("dumping the rest of the stream...");
        loop {
            let mut mem_size = 0;
            let mut chunk = vec![];
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
                    // soooooo if we're setting the db cache to max_size and then letting
                    // multiple chunks in the queue that are >= max_size, then at any time
                    // we might be using some multiple of max_size?
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

        let walker = Walker::new(commit.data);

        Ok((
            commit,
            BigCarReady {
                process: self.process,
                store,
                walker,
            },
        ))
    }
}

pub struct BigCarReady<T: Clone> {
    process: fn(Vec<u8>) -> T,
    store: SqliteStore,
    walker: Walker,
}

impl<T: Processable + Send + 'static> BigCarReady<T> {
    pub async fn next_chunk(
        mut self,
        n: usize,
    ) -> Result<(Self, Option<BlockChunk<T>>), DriveError> {
        let mut out = Vec::with_capacity(n);
        (self, out) = tokio::task::spawn_blocking(move || {
            let store = self.store;
            let mut reader = store.get_reader()?;

            for _ in 0..n {
                // walk as far as we can until we run out of blocks or find a record
                match self.walker.disk_step(&mut reader, self.process)? {
                    Step::Missing(cid) => return Err(DriveError::MissingBlock(cid)),
                    Step::Finish => break,
                    Step::Step { rkey, data } => {
                        out.push((rkey, data));
                        continue;
                    }
                };
            }

            drop(reader); // cannot outlive store
            self.store = store;
            Ok::<_, DriveError>((self, out))
        })
        .await??;

        if out.is_empty() {
            Ok((self, None))
        } else {
            Ok((self, Some(out)))
        }
    }

    fn read_tx_blocking(
        mut self,
        n: usize,
        tx: mpsc::Sender<Result<BlockChunk<T>, DriveError>>,
    ) -> Result<(), mpsc::error::SendError<Result<BlockChunk<T>, DriveError>>> {
        let mut reader = match self.store.get_reader() {
            Ok(r) => r,
            Err(e) => return tx.blocking_send(Err(e.into())),
        };

        loop {
            let mut out: BlockChunk<T> = Vec::with_capacity(n);

            for _ in 0..n {
                // walk as far as we can until we run out of blocks or find a record

                let step = match self.walker.disk_step(&mut reader, self.process) {
                    Ok(s) => s,
                    Err(e) => return tx.blocking_send(Err(e.into())),
                };

                match step {
                    Step::Missing(cid) => {
                        return tx.blocking_send(Err(DriveError::MissingBlock(cid)));
                    }
                    Step::Finish => return Ok(()),
                    Step::Step { rkey, data } => {
                        out.push((rkey, data));
                        continue;
                    }
                };
            }

            if out.is_empty() {
                break;
            }
            tx.blocking_send(Ok(out))?;
        }

        Ok(())
    }

    pub fn to_channel(self, n: usize) -> mpsc::Receiver<Result<BlockChunk<T>, DriveError>> {
        let (tx, rx) = mpsc::channel::<Result<BlockChunk<T>, DriveError>>(1);

        // sketch: this worker is going to be allowed to execute without a join handle
        tokio::task::spawn_blocking(move || {
            if let Err(mpsc::error::SendError(_)) = self.read_tx_blocking(n, tx) {
                log::debug!("big car reader exited early due to dropped receiver channel");
            }
        });

        rx
    }
}
