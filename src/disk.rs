use crate::drive::DriveError;
use rusqlite::OptionalExtension;
use std::path::PathBuf;

pub struct SqliteStore {
    path: PathBuf,
    limit_mb: usize,
}

impl SqliteStore {
    pub fn new(path: PathBuf, limit_mb: usize) -> Self {
        Self { path, limit_mb }
    }
}

impl SqliteStore {
    pub async fn get_access(&mut self) -> Result<SqliteAccess, rusqlite::Error> {
        let path = self.path.clone();
        let limit_mb = self.limit_mb;
        let conn = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(path)?;

            let sq_mb = -(2_i64.pow(10)); // negative is kibibytes for sqlite cache_size

            // conn.pragma_update(None, "journal_mode", "OFF")?;
            // conn.pragma_update(None, "journal_mode", "MEMORY")?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            // conn.pragma_update(None, "wal_autocheckpoint", "0")?; // this lets things get a bit big on disk
            conn.pragma_update(None, "synchronous", "OFF")?;
            conn.pragma_update(None, "cache_size", (limit_mb as i64 * sq_mb).to_string())?;
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

        Ok(SqliteAccess { conn })
    }
}

pub struct SqliteAccess {
    conn: rusqlite::Connection,
}

impl SqliteAccess {
    pub fn get_writer(&'_ mut self) -> Result<SqliteWriter<'_>, rusqlite::Error> {
        let tx = self.conn.transaction()?;
        // let insert_stmt = tx.prepare("INSERT INTO blocks (key, val) VALUES (?1, ?2)")?;
        Ok(SqliteWriter { tx: Some(tx) })
    }
    pub fn get_reader(&'_ self) -> Result<SqliteReader<'_>, rusqlite::Error> {
        let select_stmt = self.conn.prepare("SELECT val FROM blocks WHERE key = ?1")?;
        Ok(SqliteReader { select_stmt })
    }
}

pub struct SqliteWriter<'conn> {
    tx: Option<rusqlite::Transaction<'conn>>,
}

/// oops careful in async
impl Drop for SqliteWriter<'_> {
    fn drop(&mut self) {
        let tx = self.tx.take();
        tx.unwrap().commit().unwrap();
    }
}

impl SqliteWriter<'_> {
    pub fn put(&mut self, key: Vec<u8>, val: Vec<u8>) -> rusqlite::Result<()> {
        let tx = self.tx.as_ref().unwrap();
        let mut insert_stmt = tx.prepare_cached("INSERT INTO blocks (key, val) VALUES (?1, ?2)")?;
        insert_stmt.execute((key, val))?;
        Ok(())
    }
    pub fn put_many(
        &mut self,
        kv: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), DriveError>>,
    ) -> Result<(), DriveError> {
        let tx = self.tx.as_ref().unwrap();
        let mut insert_stmt = tx.prepare_cached("INSERT INTO blocks (key, val) VALUES (?1, ?2)")?;
        for pair in kv {
            let (k, v) = pair?;
            insert_stmt.execute((k, v))?;
        }
        Ok(())
    }
}

pub struct SqliteReader<'conn> {
    select_stmt: rusqlite::Statement<'conn>,
}

impl SqliteReader<'_> {
    pub fn get(&mut self, key: Vec<u8>) -> rusqlite::Result<Option<Vec<u8>>> {
        self.select_stmt
            .query_one((&key,), |row| row.get(0))
            .optional()
    }
}
