//! Consume a CAR from an AsyncRead, producing an ordered stream of records

use crate::disk::{DiskError, DiskStore};
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
    StorageError(#[from] DiskError),
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

/// An in-order chunk of Rkey + (processed) Block pairs
pub type BlockChunk<T> = Vec<(String, T)>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MaybeProcessedBlock<T> {
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

impl<T> MaybeProcessedBlock<T> {
    fn maybe(process: fn(Vec<u8>) -> T, data: Vec<u8>) -> Self {
        if Node::could_be(&data) {
            MaybeProcessedBlock::Raw(data)
        } else {
            MaybeProcessedBlock::Processed(process(data))
        }
    }
}

/// Read a CAR file, buffering blocks in memory or to disk
pub enum Driver<R: AsyncRead + Unpin, T: Processable> {
    /// All blocks fit within the memory limit
    ///
    /// You probably want to check the commit's signature. You can go ahead and
    /// walk the MST right away.
    Memory(Commit, MemDriver<T>),
    /// Blocks exceed the memory limit
    ///
    /// You'll need to provide a disk storage to continue. The commit will be
    /// returned and can be validated only once all blocks are loaded.
    Disk(NeedDisk<R, T>),
}

/// Builder-style driver setup
#[derive(Debug, Clone)]
pub struct DriverBuilder {
    pub mem_limit_mb: usize,
}

impl Default for DriverBuilder {
    fn default() -> Self {
        Self { mem_limit_mb: 16 }
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
    pub fn with_mem_limit_mb(self, new_limit: usize) -> Self {
        Self {
            mem_limit_mb: new_limit,
        }
    }
    /// Set the block processor
    ///
    /// Default: noop, raw blocks will be emitted
    pub fn with_block_processor<T: Processable>(
        self,
        p: fn(Vec<u8>) -> T,
    ) -> DriverBuilderWithProcessor<T> {
        DriverBuilderWithProcessor {
            mem_limit_mb: self.mem_limit_mb,
            block_processor: p,
        }
    }
    /// Begin processing an atproto MST from a CAR file
    pub async fn load_car<R: AsyncRead + Unpin>(
        &self,
        reader: R,
    ) -> Result<Driver<R, Vec<u8>>, DriveError> {
        Driver::load_car(reader, crate::process::noop, self.mem_limit_mb).await
    }
}

/// Builder-style driver intermediate step
///
/// start from `DriverBuilder`
#[derive(Debug, Clone)]
pub struct DriverBuilderWithProcessor<T: Processable> {
    pub mem_limit_mb: usize,
    pub block_processor: fn(Vec<u8>) -> T,
}

impl<T: Processable> DriverBuilderWithProcessor<T> {
    /// Set the in-memory size limit, in MiB
    ///
    /// Default: 16 MiB
    pub fn with_mem_limit_mb(mut self, new_limit: usize) -> Self {
        self.mem_limit_mb = new_limit;
        self
    }
    /// Begin processing an atproto MST from a CAR file
    pub async fn load_car<R: AsyncRead + Unpin>(
        &self,
        reader: R,
    ) -> Result<Driver<R, T>, DriveError> {
        Driver::load_car(reader, self.block_processor, self.mem_limit_mb).await
    }
}

impl<R: AsyncRead + Unpin, T: Processable> Driver<R, T> {
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
        process: fn(Vec<u8>) -> T,
        mem_limit_mb: usize,
    ) -> Result<Driver<R, T>, DriveError> {
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
            mem_size += std::mem::size_of::<Cid>() + maybe_processed.get_size();
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

        let walker = Walker::new(commit.data);

        Ok(Driver::Memory(
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
    /// Step through the record outputs, in rkey order
    pub async fn next_chunk(&mut self, n: usize) -> Result<Option<BlockChunk<T>>, DriveError> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            // walk as far as we can until we run out of blocks or find a record
            match self.walker.step(&mut self.blocks, self.process)? {
                Step::Missing(cid) => return Err(DriveError::MissingBlock(cid)),
                Step::Finish => break,
                Step::Found { rkey, data } => {
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

/// A partially memory-loaded car file that needs disk spillover to continue
pub struct NeedDisk<R: AsyncRead + Unpin, T: Processable> {
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

pub(crate) fn decode<T: Processable>(bytes: &[u8]) -> Result<T, DecodeError> {
    let (t, n) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    if n != bytes.len() {
        return Err(DecodeError::ExtraGarbage);
    }
    Ok(t)
}

impl<R: AsyncRead + Unpin, T: Processable + Send + 'static> NeedDisk<R, T> {
    pub async fn finish_loading(
        mut self,
        mut store: DiskStore,
    ) -> Result<(Commit, DiskDriver<T>), DriveError> {
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

        let (tx, mut rx) = mpsc::channel::<Vec<(Cid, MaybeProcessedBlock<T>)>>(1);

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
                let maybe_processed = MaybeProcessedBlock::maybe(self.process, data);
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
pub struct DiskDriver<T: Clone> {
    process: fn(Vec<u8>) -> T,
    state: Option<BigState>,
}

// for doctests only
#[doc(hidden)]
pub fn _get_fake_disk_driver() -> DiskDriver<Vec<u8>> {
    use crate::process::noop;
    DiskDriver {
        process: noop,
        state: None,
    }
}

impl<T: Processable + Send + 'static> DiskDriver<T> {
    /// Walk the MST returning up to `n` rkey + record pairs
    ///
    /// ```no_run
    /// # use repo_stream::{drive::{DiskDriver, DriveError, _get_fake_disk_driver}, process::noop};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DriveError> {
    /// # let mut disk_driver = _get_fake_disk_driver();
    /// while let Some(pairs) = disk_driver.next_chunk(256).await? {
    ///     for (rkey, record) in pairs {
    ///         println!("{rkey}: size={}", record.len());
    ///     }
    /// }
    /// let store = disk_driver.reset_store().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_chunk(&mut self, n: usize) -> Result<Option<BlockChunk<T>>, DriveError> {
        let process = self.process;

        // state should only *ever* be None transiently while inside here
        let mut state = self.state.take().expect("DiskDriver must have Some(state)");

        // the big pain here is that we don't want to leave self.state in an
        // invalid state (None), so all the error paths have to make sure it
        // comes out again.
        let (state, res) = tokio::task::spawn_blocking(
            move || -> (BigState, Result<BlockChunk<T>, DriveError>) {
                let mut reader_res = state.store.get_reader();
                let reader: &mut _ = match reader_res {
                    Ok(ref mut r) => r,
                    Err(ref mut e) => {
                        // unfortunately we can't return the error directly because
                        // (for some reason) it's attached to the lifetime of the
                        // reader?
                        // hack a mem::swap so we can get it out :/
                        let e_swapped = e.steal();
                        // the pain: `state` *has to* outlive the reader
                        drop(reader_res);
                        return (state, Err(e_swapped.into()));
                    }
                };

                let mut out = Vec::with_capacity(n);

                for _ in 0..n {
                    // walk as far as we can until we run out of blocks or find a record
                    let step = match state.walker.disk_step(reader, process) {
                        Ok(s) => s,
                        Err(e) => {
                            // the pain: `state` *has to* outlive the reader
                            drop(reader_res);
                            return (state, Err(e.into()));
                        }
                    };
                    match step {
                        Step::Missing(cid) => {
                            // the pain: `state` *has to* outlive the reader
                            drop(reader_res);
                            return (state, Err(DriveError::MissingBlock(cid)));
                        }
                        Step::Finish => break,
                        Step::Found { rkey, data } => out.push((rkey, data)),
                    };
                }

                // `state` *has to* outlive the reader
                drop(reader_res);

                (state, Ok::<_, DriveError>(out))
            },
        )
        .await?; // on tokio JoinError, we'll be left with invalid state :(

        // *must* restore state before dealing with the actual result
        self.state = Some(state);

        let out = res?;

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    fn read_tx_blocking(
        &mut self,
        n: usize,
        tx: mpsc::Sender<Result<BlockChunk<T>, DriveError>>,
    ) -> Result<(), mpsc::error::SendError<Result<BlockChunk<T>, DriveError>>> {
        let BigState { store, walker } = self.state.as_mut().expect("valid state");
        let mut reader = match store.get_reader() {
            Ok(r) => r,
            Err(e) => return tx.blocking_send(Err(e.into())),
        };

        loop {
            let mut out: BlockChunk<T> = Vec::with_capacity(n);

            for _ in 0..n {
                // walk as far as we can until we run out of blocks or find a record

                let step = match walker.disk_step(&mut reader, self.process) {
                    Ok(s) => s,
                    Err(e) => return tx.blocking_send(Err(e.into())),
                };

                match step {
                    Step::Missing(cid) => {
                        return tx.blocking_send(Err(DriveError::MissingBlock(cid)));
                    }
                    Step::Finish => return Ok(()),
                    Step::Found { rkey, data } => {
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
    /// # use repo_stream::{drive::{DiskDriver, DriveError, _get_fake_disk_driver}, process::noop};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DriveError> {
    /// # let mut disk_driver = _get_fake_disk_driver();
    /// let (mut rx, join) = disk_driver.to_channel(512);
    /// while let Some(recvd) = rx.recv().await {
    ///     let pairs = recvd?;
    ///     for (rkey, record) in pairs {
    ///         println!("{rkey}: size={}", record.len());
    ///     }
    ///
    /// }
    /// let store = join.await?.reset_store().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_channel(
        mut self,
        n: usize,
    ) -> (
        mpsc::Receiver<Result<BlockChunk<T>, DriveError>>,
        tokio::task::JoinHandle<Self>,
    ) {
        let (tx, rx) = mpsc::channel::<Result<BlockChunk<T>, DriveError>>(1);

        // sketch: this worker is going to be allowed to execute without a join handle
        let chan_task = tokio::task::spawn_blocking(move || {
            if let Err(mpsc::error::SendError(_)) = self.read_tx_blocking(n, tx) {
                log::debug!("big car reader exited early due to dropped receiver channel");
            }
            self
        });

        (rx, chan_task)
    }

    /// Reset the disk storage so it can be reused. You must call this.
    ///
    /// Ideally we'd put this in an `impl Drop`, but since it makes blocking
    /// calls, that would be risky in an async context. For now you just have to
    /// carefully make sure you call it.
    ///
    /// The sqlite store is returned, so it can be reused for another
    /// `DiskDriver`.
    pub async fn reset_store(mut self) -> Result<DiskStore, DriveError> {
        let BigState { store, .. } = self.state.take().expect("valid state");
        Ok(store.reset().await?)
    }
}
