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
use rusqlite::OptionalExtension;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    /// A wrapped database error
    ///
    /// (The wrapped err should probably be obscured to remove public-facing
    /// sqlite bits)
    #[error(transparent)]
    DbError(#[from] rusqlite::Error),
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
            cache_size_mb: 32,
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
    /// Default: 32 MiB
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
    conn: rusqlite::Connection,
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
        let conn = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(path)?;

            let sqlite_one_mb = -(2_i64.pow(10)); // negative is kibibytes for sqlite cache_size

            // conn.pragma_update(None, "journal_mode", "OFF")?;
            // conn.pragma_update(None, "journal_mode", "MEMORY")?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            // conn.pragma_update(None, "wal_autocheckpoint", "0")?; // this lets things get a bit big on disk
            conn.pragma_update(None, "synchronous", "OFF")?;
            conn.pragma_update(
                None,
                "cache_size",
                (cache_mb as i64 * sqlite_one_mb).to_string(),
            )?;
            Self::reset_tables(&conn)?;

            Ok::<_, DiskError>(conn)
        })
        .await??;

        Ok(Self {
            conn,
            max_stored,
            stored: 0,
        })
    }
    pub(crate) fn get_writer(&'_ mut self) -> Result<SqliteWriter<'_>, DiskError> {
        let tx = self.conn.transaction()?;
        Ok(SqliteWriter {
            tx,
            stored: &mut self.stored,
            max: self.max_stored,
        })
    }
    pub(crate) fn get_reader<'conn>(&'conn self) -> Result<SqliteReader<'conn>, DiskError> {
        let select_stmt = self.conn.prepare("SELECT val FROM blocks WHERE key = ?1")?;
        Ok(SqliteReader { select_stmt })
    }
    /// Drop and recreate the kv table
    pub async fn reset(self) -> Result<Self, DiskError> {
        tokio::task::spawn_blocking(move || {
            Self::reset_tables(&self.conn)?;
            Ok(self)
        })
        .await?
    }
    fn reset_tables(conn: &rusqlite::Connection) -> Result<(), DiskError> {
        conn.execute("DROP TABLE IF EXISTS blocks", ())?;
        conn.execute(
            "CREATE TABLE blocks (
                key  BLOB PRIMARY KEY NOT NULL,
                val  BLOB NOT NULL
            ) WITHOUT ROWID",
            (),
        )?;
        Ok(())
    }
}

pub(crate) struct SqliteWriter<'conn> {
    tx: rusqlite::Transaction<'conn>,
    stored: &'conn mut usize,
    max: usize,
}

impl SqliteWriter<'_> {
    pub(crate) fn put_many(
        &mut self,
        kv: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), DriveError>>,
    ) -> Result<(), DriveError> {
        let mut insert_stmt = self
            .tx
            .prepare_cached("INSERT INTO blocks (key, val) VALUES (?1, ?2)")
            .map_err(DiskError::DbError)?;
        for pair in kv {
            let (k, v) = pair?;
            *self.stored += v.len();
            if *self.stored > self.max {
                return Err(DiskError::MaxSizeExceeded.into());
            }
            insert_stmt.execute((k, v)).map_err(DiskError::DbError)?;
        }
        Ok(())
    }
    pub fn commit(self) -> Result<(), DiskError> {
        self.tx.commit()?;
        Ok(())
    }
}

pub(crate) struct SqliteReader<'conn> {
    select_stmt: rusqlite::Statement<'conn>,
}

impl SqliteReader<'_> {
    pub(crate) fn get(&mut self, key: Vec<u8>) -> rusqlite::Result<Option<Vec<u8>>> {
        self.select_stmt
            .query_one((&key,), |row| row.get(0))
            .optional()
    }
}
