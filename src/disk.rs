/*!
Disk storage for blocks on disk

Currently this uses sqlite. In testing sqlite wasn't the fastest, but it seemed
to be the best behaved in terms of both on-disk space usage and memory usage.

```no_run
# use repo_stream::{DiskBuilder, DiskError};
# #[tokio::main]
# async fn main() -> Result<(), DiskError> {
let store = DiskBuilder::new()
    .with_cache_size_mb(32)
    .with_max_stored_mb(1024) // errors when >1GiB of processed blocks are inserted
    .open("/some/path.db".into()).await?;
# Ok(())
# }
```
*/

use crate::drive::DriveError;
use fjall::{Config, Error as FjallError, Keyspace, Partition, PartitionCreateOptions};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    /// A wrapped database error
    ///
    /// (The wrapped err should probably be obscured to remove public-facing
    /// sqlite bits)
    #[error(transparent)]
    DbError(#[from] FjallError),
    /// A tokio blocking task failed to join
    #[error("Failed to join a tokio blocking task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    /// The total size of stored blocks exceeded the allowed size
    ///
    /// If you need to process *really* big CARs, you can configure a higher
    /// limit.
    #[error("Maximum disk size reached")]
    MaxSizeExceeded,
}

/// Builder-style disk store setup
#[derive(Debug, Clone)]
pub struct DiskBuilder {
    /// Database in-memory cache allowance
    ///
    /// Default: 32 MiB
    pub cache_size_mb: usize,
    /// Database stored block size limit
    ///
    /// Default: 10 GiB
    ///
    /// Note: actual size on disk may be more, but should approximately scale
    /// with this limit
    pub max_stored_mb: usize,
}

impl Default for DiskBuilder {
    fn default() -> Self {
        Self {
            cache_size_mb: 64,
            max_stored_mb: 10 * 1024, // 10 GiB
        }
    }
}

impl DiskBuilder {
    /// Begin configuring the storage with defaults
    pub fn new() -> Self {
        Default::default()
    }
    /// Set the in-memory cache allowance for the database
    ///
    /// Default: 64 MiB
    pub fn with_cache_size_mb(mut self, size: usize) -> Self {
        self.cache_size_mb = size;
        self
    }
    /// Set the approximate stored block size limit
    ///
    /// Default: 10 GiB
    pub fn with_max_stored_mb(mut self, max: usize) -> Self {
        self.max_stored_mb = max;
        self
    }
    /// Open and initialize the actual disk storage
    pub async fn open(&self, path: PathBuf) -> Result<DiskStore, DiskError> {
        DiskStore::new(path, self.cache_size_mb, self.max_stored_mb).await
    }
}

/// On-disk block storage
pub struct DiskStore {
    #[allow(unused)]
    db: Keyspace,
    partition: Partition,
    max_stored: usize,
    stored: usize,
}

impl DiskStore {
    /// Initialize a new disk store
    pub async fn new(
        path: PathBuf,
        cache_mb: usize,
        max_stored_mb: usize,
    ) -> Result<Self, DiskError> {
        let max_stored = max_stored_mb * 2_usize.pow(20);
        let (db, partition) = tokio::task::spawn_blocking(move || {
            let db = Config::new(path)
                // .manual_journal_persist(true)
                // .flush_workers(1)
                // .compaction_workers(1)
                .cache_size(cache_mb as u64 * 2_u64.pow(20))
                .temporary(true)
                .open()?;
            let partition = Self::get_partition(&db)?;

            Ok::<_, DiskError>((db, partition))
        })
        .await??;

        Ok(Self {
            db,
            partition,
            max_stored,
            stored: 0,
        })
    }

    /// Drop and recreate the kv table
    pub async fn reset(mut self) -> Result<Self, DiskError> {
        tokio::task::spawn_blocking(move || {
            let partition = self.partition;
            Self::reset_partition(&self.db, partition)?;
            self.partition = Self::get_partition(&self.db)?;
            Ok(self)
        })
        .await?
    }

    fn get_partition(db: &Keyspace) -> Result<Partition, FjallError> {
        db.open_partition("z", PartitionCreateOptions::default())
    }
    fn reset_partition(keyspace: &Keyspace, partition: Partition) -> Result<Partition, DiskError> {
        keyspace.delete_partition(partition)?;
        let partition = Self::get_partition(keyspace)?;
        Ok(partition)
    }
    pub(crate) fn put_many(
        &mut self,
        kv: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), DriveError>>,
    ) -> Result<(), DriveError> {
        let mut batch = self.db.batch();
        for pair in kv {
            let (k, v) = pair?;
            self.stored += v.len();
            if self.stored > self.max_stored {
                return Err(DiskError::MaxSizeExceeded.into());
            }
            batch.insert(&self.partition, k, v);
        }
        batch.commit().map_err(DiskError::DbError)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn get(&mut self, key: &[u8]) -> Result<Option<fjall::Slice>, FjallError> {
        self.partition.get(key)
    }
}
