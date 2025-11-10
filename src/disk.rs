/*!
Sqlite storage for blocks on disk

In testing sqlite wasn't the fastest, but it seemed to be the best behaved in
terms of both on-disk space usage and memory usage.

```no_run
# use repo_stream::SqliteStore;
# #[tokio::main]
# async fn main() -> Result<(), rusqlite::Error> {
let db_cache_size = 32; // MiB
let store = SqliteStore::new("/some/path.sqlite".into(), db_cache_size).await?;
# Ok(())
# }
```
*/

use crate::drive::DriveError;
use rusqlite::OptionalExtension;
use std::path::PathBuf;

/// On-disk block storage
pub struct SqliteStore {
    conn: rusqlite::Connection,
}

impl SqliteStore {
    pub async fn new(path: PathBuf, cache_mb: usize) -> Result<Self, rusqlite::Error> {
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
            conn.execute(
                "CREATE TABLE blocks (
                    key  BLOB PRIMARY KEY NOT NULL,
                    val  BLOB NOT NULL
                ) WITHOUT ROWID",
                (),
            )?;

            Ok::<_, rusqlite::Error>(conn)
        })
        .await
        .expect("join error")?;

        Ok(Self { conn })
    }
    pub(crate) fn get_writer(&'_ mut self) -> Result<SqliteWriter<'_>, rusqlite::Error> {
        let tx = self.conn.transaction()?;
        // let insert_stmt = tx.prepare("INSERT INTO blocks (key, val) VALUES (?1, ?2)")?;
        Ok(SqliteWriter { tx })
    }
    pub(crate) fn get_reader<'conn>(&'conn self) -> Result<SqliteReader<'conn>, rusqlite::Error> {
        let select_stmt = self.conn.prepare("SELECT val FROM blocks WHERE key = ?1")?;
        Ok(SqliteReader { select_stmt })
    }
    pub fn reset(&mut self) -> Result<(), rusqlite::Error> {
        self.conn.execute("DROP TABLE blocks", ())?;
        Ok(())
    }
}

pub(crate) struct SqliteWriter<'conn> {
    tx: rusqlite::Transaction<'conn>,
}

impl SqliteWriter<'_> {
    pub fn put_many(
        &mut self,
        kv: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), DriveError>>,
    ) -> Result<(), DriveError> {
        let mut insert_stmt = self
            .tx
            .prepare_cached("INSERT INTO blocks (key, val) VALUES (?1, ?2)")?;
        for pair in kv {
            let (k, v) = pair?;
            insert_stmt.execute((k, v))?;
        }
        Ok(())
    }
    pub fn commit(self) -> Result<(), rusqlite::Error> {
        self.tx.commit()?;
        Ok(())
    }
}

pub(crate) struct SqliteReader<'conn> {
    select_stmt: rusqlite::Statement<'conn>,
}

impl SqliteReader<'_> {
    pub fn get(&mut self, key: Vec<u8>) -> rusqlite::Result<Option<Vec<u8>>> {
        self.select_stmt
            .query_one((&key,), |row| row.get(0))
            .optional()
    }
}
