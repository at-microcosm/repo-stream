use redb::ReadableDatabase;
use rusqlite::OptionalExtension;
use std::error::Error;
use std::path::PathBuf;

pub trait StorageErrorBase: Error + Send + 'static {}

/// high level potential storage resource
///
/// separating this allows (hopefully) implementing a storage pool that can
/// async-block when until a member is available to use
pub trait DiskStore {
    type StorageError: StorageErrorBase + Send;
    type Access: DiskAccess<StorageError = Self::StorageError>;
    fn get_access(&mut self) -> impl Future<Output = Result<Self::Access, Self::StorageError>>;
}

/// actual concrete access to disk storage
pub trait DiskAccess: Send {
    type StorageError: StorageErrorBase;

    fn get_writer(&mut self) -> Result<impl DiskWriter<Self::StorageError>, Self::StorageError>;

    fn get_reader(
        &self,
    ) -> Result<impl DiskReader<StorageError = Self::StorageError>, Self::StorageError>;

    // TODO: force a cleanup implementation?
}

pub trait DiskWriter<E: StorageErrorBase> {
    fn put(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<(), E>;
}

pub trait DiskReader {
    type StorageError: StorageErrorBase;
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Self::StorageError>;
}

///////////////// sqlite

pub struct SqliteStore {
    path: PathBuf,
}

impl SqliteStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl StorageErrorBase for rusqlite::Error {}

impl DiskStore for SqliteStore {
    type StorageError = rusqlite::Error;
    type Access = SqliteAccess;
    async fn get_access(&mut self) -> Result<SqliteAccess, rusqlite::Error> {
        let path = self.path.clone();
        let conn = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(path)?;

            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "OFF")?;
            conn.pragma_update(None, "cache_size", (-4 * 2_i64.pow(10)).to_string())?;
            conn.execute(
                "CREATE TABLE blocks (
                    key  BLOB PRIMARY KEY NOT NULL,
                    val  BLOB NOT NULL
                ) WITHOUT ROWID",
                (),
            )?;

            Ok::<_, Self::StorageError>(conn)
        })
        .await
        .expect("join error")?;

        Ok(SqliteAccess { conn })
    }
}

pub struct SqliteAccess {
    conn: rusqlite::Connection,
}

impl DiskAccess for SqliteAccess {
    type StorageError = rusqlite::Error;
    fn get_writer(&mut self) -> Result<impl DiskWriter<rusqlite::Error>, rusqlite::Error> {
        let insert_stmt = self
            .conn
            .prepare("INSERT INTO blocks (key, val) VALUES (?1, ?2)")?;
        Ok(SqliteWriter { insert_stmt })
    }
    fn get_reader(
        &self,
    ) -> Result<impl DiskReader<StorageError = rusqlite::Error>, rusqlite::Error> {
        let select_stmt = self.conn.prepare("SELECT val FROM blocks WHERE key = ?1")?;
        Ok(SqliteReader { select_stmt })
    }
}

pub struct SqliteWriter<'conn> {
    insert_stmt: rusqlite::Statement<'conn>,
}

impl DiskWriter<rusqlite::Error> for SqliteWriter<'_> {
    fn put(&mut self, key: Vec<u8>, val: Vec<u8>) -> rusqlite::Result<()> {
        self.insert_stmt.execute((key, val))?;
        Ok(())
    }
}

pub struct SqliteReader<'conn> {
    select_stmt: rusqlite::Statement<'conn>,
}

impl DiskReader for SqliteReader<'_> {
    type StorageError = rusqlite::Error;
    fn get(&mut self, key: Vec<u8>) -> rusqlite::Result<Option<Vec<u8>>> {
        self.select_stmt
            .query_one((&key,), |row| row.get(0))
            .optional()
    }
}

//////////// redb why not

const REDB_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("blocks");

pub struct RedbStore {
    path: PathBuf,
}

impl RedbStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl StorageErrorBase for redb::Error {}

impl DiskStore for RedbStore {
    type StorageError = redb::Error;
    type Access = RedbAccess;
    async fn get_access(&mut self) -> Result<RedbAccess, redb::Error> {
        let path = self.path.clone();
        let kb = 2_usize.pow(10);
        let db = tokio::task::spawn_blocking(move || {
            let db = redb::Database::builder()
                .set_cache_size(16 * kb)
                .create(path)?;
            Ok::<_, Self::StorageError>(db)
        })
        .await
        .expect("join error")?;

        Ok(RedbAccess { db })
    }
}

pub struct RedbAccess {
    db: redb::Database,
}

impl DiskAccess for RedbAccess {
    type StorageError = redb::Error;
    fn get_writer(&mut self) -> Result<impl DiskWriter<redb::Error>, redb::Error> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(redb::Durability::None)?;
        Ok(RedbWriter { tx: Some(tx) })
    }
    fn get_reader(&self) -> Result<impl DiskReader<StorageError = redb::Error>, redb::Error> {
        let tx = self.db.begin_read()?;
        Ok(RedbReader { tx })
    }
}

pub struct RedbWriter {
    tx: Option<redb::WriteTransaction>,
}

impl DiskWriter<redb::Error> for RedbWriter {
    fn put(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<(), redb::Error> {
        let mut table = self.tx.as_ref().unwrap().open_table(REDB_TABLE)?;
        table.insert(&*key, &*val)?;
        Ok(())
    }
}

/// oops careful in async
impl Drop for RedbWriter {
    fn drop(&mut self) {
        let tx = self.tx.take();
        tx.unwrap().commit().unwrap();
    }
}

pub struct RedbReader {
    tx: redb::ReadTransaction,
}

impl DiskReader for RedbReader {
    type StorageError = redb::Error;
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, redb::Error> {
        let table = self.tx.open_table(REDB_TABLE)?;
        let rv = table.get(&*key)?.map(|guard| guard.value().to_vec());
        Ok(rv)
    }
}

///// rustcask??

pub struct RustcaskStore {
    path: PathBuf,
}

impl RustcaskStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CaskError {
    #[error(transparent)]
    OpenError(#[from] rustcask::error::OpenError),
    #[error(transparent)]
    SetError(#[from] rustcask::error::SetError),
    #[error("failed to get key: {0}")]
    GetError(String),
    #[error("failed to ensure directory: {0}")]
    EnsureDirError(std::io::Error),
}

impl StorageErrorBase for CaskError {}

impl DiskStore for RustcaskStore {
    type StorageError = CaskError;
    type Access = RustcaskAccess;
    async fn get_access(&mut self) -> Result<RustcaskAccess, CaskError> {
        let path = self.path.clone();
        let db = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&path).map_err(CaskError::EnsureDirError)?;
            let db = rustcask::Rustcask::builder().open(&path)?;
            Ok::<_, Self::StorageError>(db)
        })
        .await
        .expect("join error")?;

        Ok(RustcaskAccess { db })
    }
}

pub struct RustcaskAccess {
    db: rustcask::Rustcask,
}

impl DiskAccess for RustcaskAccess {
    type StorageError = CaskError;
    fn get_writer(&mut self) -> Result<impl DiskWriter<CaskError>, CaskError> {
        Ok(RustcaskWriter { db: self.db.clone() })
    }
    fn get_reader(&self) -> Result<impl DiskReader<StorageError = CaskError>, CaskError> {
        Ok(RustcaskReader { db: self.db.clone() })
    }
}

pub struct RustcaskWriter {
    db: rustcask::Rustcask,
}

impl DiskWriter<CaskError> for RustcaskWriter {
    fn put(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<(), CaskError> {
        self.db.set(key, val)?;
        Ok(())
    }
}

pub struct RustcaskReader {
    db: rustcask::Rustcask,
}

impl DiskReader for RustcaskReader {
    type StorageError = CaskError;
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, CaskError> {
        self.db
            .get(&key)
            .map_err(|e| CaskError::GetError(e.to_string()))
    }
}
