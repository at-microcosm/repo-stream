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

/////////////////

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
            conn.pragma_update(None, "cache_size", (-32 * 2_i64.pow(10)).to_string())?;
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

// /// The main storage interface for MST blocks
// ///
// /// **Note**: `get` and `put` are **synchronous methods that may block**
// pub trait BlockStore<T: Clone> {
//     fn get(&self, cid: Cid) -> Option<MaybeProcessedBlock<T>>;
//     fn put(&mut self, cid: Cid, mpb: MaybeProcessedBlock<T>);
// }

// ///// wheee

// /// In-memory MST block storage
// ///
// /// a thin wrapper around a hashmap
// pub struct MemoryStore<T: Clone> {
//     map: HashMap<Cid, MaybeProcessedBlock<T>>,
// }

// impl<T: Clone> BlockStore<T> for MemoryStore<T> {
//     fn get(&self, cid: Cid) -> Option<MaybeProcessedBlock<T>> {
//         self.map.get(&cid).map(|t| t.clone())
//     }
//     fn put(&mut self, cid: Cid, mpb: MaybeProcessedBlock<T>) {
//         self.map.insert(cid, mpb);
//     }
// }

// //// the fun bits

// pub struct HybridStore<T: Clone, D: DiskStore> {
//     mem: MemoryStore<T>,
//     disk: D,
// }

// impl<T: Clone, D: DiskStore> BlockStore<T> for HybridStore<T, D> {
//     fn get(&self, _cid: Cid) -> Option<MaybeProcessedBlock<T>> { todo!() }
//     fn put(&mut self, _cid: Cid, _mpb: MaybeProcessedBlock<T>) { todo!() }
// }
