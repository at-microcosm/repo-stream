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
use fjall::{Database, Keyspace, KeyspaceCreateOptions, Error as FjallError};
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
    #[error("this error was replaced, seeing this is a bug.")]
    #[doc(hidden)]
    Stolen,
}

impl DiskError {
    /// hack for ownership challenges with the disk driver
    pub(crate) fn steal(&mut self) -> Self {
        let mut swapped = DiskError::Stolen;
        std::mem::swap(self, &mut swapped);
        swapped
    }
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
    db: Database,
    ks: Keyspace,
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
        let (db, ks) = tokio::task::spawn_blocking(move || {
            let db = Database::builder(path)
                // .manual_journal_persist(true)
                // .worker_threads(1)
                // .cache_size(cache_mb as u64 * 2_u64.pow(20))
                // .temporary(true)
                .open()?;
            let ks = db.keyspace("z", ||
                KeyspaceCreateOptions::default()
                    // .expect_point_read_hits(true)
                    // .manual_journal_persist(true)
            )?;

            // Self::reset_tables(&ks)?;

            Ok::<_, DiskError>((db, ks))
        })
        .await??;

        Ok(Self {
            db,
            ks,
            max_stored,
            stored: 0,
        })
    }
    pub(crate) fn get_writer(&'_ mut self) -> Result<SqliteWriter<'_>, DiskError> {
        Ok(SqliteWriter {
            ks: self.ks.clone(),
            stored: &mut self.stored,
            max: self.max_stored,
        })
    }
    pub(crate) fn get_reader(&self) -> Result<SqliteReader, DiskError> {
        Ok(SqliteReader {
            ks: self.ks.clone(),
        })
    }
    /// Drop and recreate the kv table
    pub async fn reset(self) -> Result<Self, DiskError> {
        tokio::task::spawn_blocking(move || {
            Self::reset_tables(&self.ks)?;
            Ok(self)
        })
        .await?
    }
    fn reset_tables(ks: &Keyspace) -> Result<(), DiskError> {
        ks.clear()?;
        Ok(())
    }
}

pub(crate) struct SqliteWriter<'a> {
    ks: Keyspace,
    stored: &'a mut usize,
    max: usize,
}

impl SqliteWriter<'_> {
    pub(crate) fn put_many(
        &mut self,
        kv: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), DriveError>>,
    ) -> Result<(), DriveError> {
        for pair in kv {
            let (k, v) = pair?;
            *self.stored += v.len();
            if *self.stored > self.max {
                return Err(DiskError::MaxSizeExceeded.into());
            }
            self.ks.insert(k, v).map_err(DiskError::DbError)?;
        }
        Ok(())
    }
}

pub(crate) struct SqliteReader {
    ks: Keyspace,
}

impl SqliteReader {
    pub(crate) fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FjallError> {
        let rv = self
            .ks
            .get(&key)?
            .map(|v| v.as_ref().into());
        Ok(rv)
    }
}
