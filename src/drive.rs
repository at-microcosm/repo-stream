//! Consume a CAR from an AsyncRead, producing an ordered stream of records

use crate::{
    Bytes, HashMap, Rkey, Step,
    disk::{DiskError, DiskStore},
    mst::MstNode,
    walk::Output,
};
use cid::Cid;
use iroh_car::CarReader;
use std::convert::Infallible;
use tokio::{io::AsyncRead, sync::mpsc};

use crate::mst::Commit;
use crate::walk::{WalkError, Walker};

/// Errors that can happen while consuming and emitting blocks and records
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
    ChannelSendError, // SendError takes <T> which we don't need
    #[error("Failed to join a task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

/// An in-order chunk of Rkey + CID + (processed) Block
pub type BlockChunk = Vec<Output>;

#[derive(Debug, Clone)]
pub(crate) enum MaybeProcessedBlock {
    /// A block that's *probably* a Node (but we can't know yet)
    ///
    /// It *can be* a record that suspiciously looks a lot like a node, so we
    /// cannot eagerly turn it into a Node. We only know for sure what it is
    /// when we actually walk down the MST
    Raw(Bytes),
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
    Processed(Bytes),
}

impl MaybeProcessedBlock {
    pub(crate) fn maybe(process: fn(Bytes) -> Bytes, data: Bytes) -> Self {
        if MstNode::could_be(&data) {
            MaybeProcessedBlock::Raw(data)
        } else {
            MaybeProcessedBlock::Processed(process(data))
        }
    }
    pub(crate) fn len(&self) -> usize {
        match self {
            MaybeProcessedBlock::Raw(b) => b.len(),
            MaybeProcessedBlock::Processed(b) => b.len(),
        }
    }
    pub(crate) fn into_bytes(self) -> Bytes {
        match self {
            MaybeProcessedBlock::Raw(mut b) => {
                b.push(0x00);
                b
            }
            MaybeProcessedBlock::Processed(mut b) => {
                b.push(0x01);
                b
            }
        }
    }
    pub(crate) fn from_bytes(mut b: Bytes) -> Self {
        // TODO: make sure bytes is not empty, that it's explicitly 0 or 1, etc
        let suffix = b.pop().unwrap();
        if suffix == 0x00 {
            MaybeProcessedBlock::Raw(b)
        } else {
            MaybeProcessedBlock::Processed(b)
        }
    }
}

/// Read a CAR file, buffering blocks in memory or to disk
pub enum Driver<R: AsyncRead + Unpin> {
    /// All blocks fit within the memory limit
    ///
    /// You probably want to check the commit's signature. You can go ahead and
    /// walk the MST right away.
    Memory(Commit, Option<Rkey>, MemDriver),
    /// Blocks exceed the memory limit
    ///
    /// You'll need to provide a disk storage to continue. The commit will be
    /// returned and can be validated only once all blocks are loaded.
    Disk(NeedDisk<R>),
}

/// Processor that just returns the raw blocks
#[inline]
pub fn noop(block: Bytes) -> Bytes {
    block
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
            mem_limit_mb: 16,
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
    /// Default: 16 MiB
    pub fn with_mem_limit_mb(mut self, new_limit: usize) -> Self {
        self.mem_limit_mb = new_limit;
        self
    }

    /// Set the block processor
    ///
    /// Default: noop, raw blocks will be emitted
    pub fn with_block_processor(mut self, new_processor: fn(Bytes) -> Bytes) -> DriverBuilder {
        self.block_processor = new_processor;
        self
    }

    /// Begin processing an atproto MST from a CAR file
    pub async fn load_car<R: AsyncRead + Unpin>(&self, reader: R) -> Result<Driver<R>, DriveError> {
        Driver::load_car(reader, self.block_processor, self.mem_limit_mb).await
    }
}

impl<R: AsyncRead + Unpin> Driver<R> {
    /// Begin processing an atproto MST from a CAR file
    ///
    /// Blocks will be loaded, processed, and buffered in memory. If the entire
    /// processed size is under the `mem_limit_mb` limit, a `Driver::Memory`
    /// will be returned along with a `Commit` ready for validation.
    ///
    /// If the `mem_limit_mb` limit is reached before loading all blocks, the
    /// partial state will be returned as `Driver::Disk(needed)`, which can be
    /// resumed by providing a `SqliteStorage` for on-disk block storage.
    pub async fn load_car(
        reader: R,
        process: fn(Bytes) -> Bytes,
        mem_limit_mb: usize,
    ) -> Result<Driver<R>, DriveError> {
        let max_size = mem_limit_mb * 2_usize.pow(20);
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
            let maybe_processed = MaybeProcessedBlock::maybe(process, data);

            // stash (maybe processed) blocks in memory as long as we have room
            mem_size += maybe_processed.len();
            mem_blocks.insert(cid, maybe_processed);
            if mem_size >= max_size {
                return Ok(Driver::Disk(NeedDisk {
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

        // the commit always must point to a Node; empty node => empty MST special case
        let root_node: MstNode = match mem_blocks
            .get(&commit.data)
            .ok_or(DriveError::MissingCommit)?
        {
            MaybeProcessedBlock::Processed(_) => Err(WalkError::BadCommitFingerprint)?,
            MaybeProcessedBlock::Raw(bytes) => serde_ipld_dagcbor::from_slice(bytes)?,
        };
        let walker = Walker::new(root_node);

        Ok(Driver::Memory(
            commit,
            None,
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
pub struct MemDriver {
    blocks: HashMap<Cid, MaybeProcessedBlock>,
    walker: Walker,
    process: fn(Bytes) -> Bytes,
}

impl MemDriver {
    /// Step through the record outputs, in rkey order
    pub async fn next_chunk(&mut self, n: usize) -> Result<Step<BlockChunk>, DriveError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            // walk as far as we can until we run out of blocks or find a record
            let Step::Value(output) = self.walker.step(&mut self.blocks, self.process)? else {
                break;
            };
            out.push(output);
        }
        if out.is_empty() {
            Ok(Step::End(None))
        } else {
            Ok(Step::Value(out))
        }
    }
}

/// A partially memory-loaded car file that needs disk spillover to continue
pub struct NeedDisk<R: AsyncRead + Unpin> {
    car: CarReader<R>,
    root: Cid,
    process: fn(Bytes) -> Bytes,
    max_size: usize,
    mem_blocks: HashMap<Cid, MaybeProcessedBlock>,
    pub commit: Option<Commit>,
}

impl<R: AsyncRead + Unpin> NeedDisk<R> {
    pub async fn finish_loading(
        mut self,
        mut store: DiskStore,
    ) -> Result<(Commit, Option<Rkey>, DiskDriver), DriveError> {
        // move store in and back out so we can manage lifetimes
        // dump mem blocks into the store
        store = tokio::task::spawn(async move {
            let kvs = self
                .mem_blocks
                .into_iter()
                .map(|(k, v)| (k.to_bytes(), v.into_bytes()));

            store.put_many(kvs)?;
            Ok::<_, DriveError>(store)
        })
        .await??;

        let (tx, mut rx) = mpsc::channel::<Vec<(Cid, MaybeProcessedBlock)>>(1);

        let store_worker = tokio::task::spawn_blocking(move || {
            while let Some(chunk) = rx.blocking_recv() {
                let kvs = chunk
                    .into_iter()
                    .map(|(k, v)| (k.to_bytes(), v.into_bytes()));
                store.put_many(kvs)?;
            }
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

                let data = Bytes::from(data);

                // remaining possible types: node, record, other. optimistically process
                // TODO: get the actual in-memory size to compute disk spill
                let maybe_processed = MaybeProcessedBlock::maybe(self.process, data);
                mem_size += maybe_processed.len();
                chunk.push((cid, maybe_processed));
                if mem_size >= (self.max_size / 2) {
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

        // the commit always must point to a Node; empty node => empty MST special case
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

        // state should only *ever* be None transiently while inside here
        let mut state = self.state.take().expect("DiskDriver must have Some(state)");

        // the big pain here is that we don't want to leave self.state in an
        // invalid state (None), so all the error paths have to make sure it
        // comes out again.
        let (state, res) =
            tokio::task::spawn_blocking(move || -> (BigState, Result<BlockChunk, DriveError>) {
                let mut out = Vec::with_capacity(n);

                for _ in 0..n {
                    // walk as far as we can until we run out of blocks or find a record
                    let step = match state.walker.disk_step(&mut state.store, process) {
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
            .await?; // on tokio JoinError, we'll be left with invalid state :(

        // *must* restore state before dealing with the actual result
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
                // walk as far as we can until we run out of blocks or find a record

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
    /// The idea is to avoid so much sending back and forth to the blocking
    /// thread, letting a blocking task do all the disk reading work and sending
    /// records and rkeys back through an `mpsc` channel instead.
    ///
    /// This might also allow the disk work to continue while processing the
    /// records. It's still not yet clear if this method actually has much
    /// benefit over just using `.next_chunk(n)`.
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

        // sketch: this worker is going to be allowed to execute without a join handle
        let chan_task = tokio::task::spawn_blocking(move || {
            if let Err(mpsc::error::SendError(_)) = self.read_tx_blocking(n, tx) {
                log::debug!("big car reader exited early due to dropped receiver channel");
            }
            self
        });

        (rx, chan_task)
    }

    /// Reset the disk storage so it can be reused.
    ///
    /// The store is returned, so it can be reused for another `DiskDriver`.
    pub async fn reset_store(mut self) -> Result<DiskStore, DriveError> {
        let BigState { store, .. } = self.state.take().expect("valid state");
        store.reset().await?;
        Ok(store)
    }
}
