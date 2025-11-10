/*!
Disk storage for blocks on disk

Currently this uses sqlite. In testing sqlite wasn't the fastest, but it seemed
to be the best behaved in terms of both on-disk space usage and memory usage.

```no_run
# use repo_stream::{DiskStore, DiskError};
# #[tokio::main]
# async fn main() -> Result<(), DiskError> {
let db_cache_size = 32; // MiB
let store = DiskStore::new("/some/path.db".into(), db_cache_size).await?;
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

/// On-disk block storage
pub struct DiskStore {
    conn: rusqlite::Connection,
}

impl DiskStore {
    /// Initialize a new disk store
    pub async fn new(path: PathBuf, cache_mb: usize) -> Result<Self, DiskError> {
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

        Ok(Self { conn })
    }
    pub(crate) fn get_writer(&'_ mut self) -> Result<SqliteWriter<'_>, DiskError> {
        let tx = self.conn.transaction()?;
        Ok(SqliteWriter { tx })
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
