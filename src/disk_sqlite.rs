use crate::disk_drive::BlockStore;
use ipld_core::cid::Cid;
use rusqlite::{Connection, OptionalExtension, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;

pub struct SqliteStore {
    conn: Connection,
}

impl SqliteStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
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

        Ok(Self { conn })
    }
}

impl Drop for SqliteStore {
    fn drop(&mut self) {
        self.conn.execute("DROP TABLE blocks", ()).unwrap();
    }
}

impl<MPB: Serialize + DeserializeOwned> BlockStore<MPB> for SqliteStore {
    fn put(&self, c: Cid, t: MPB) {
        let key_bytes = c.to_bytes();
        let val_bytes = bincode::serde::encode_to_vec(t, bincode::config::standard()).unwrap();

        self.conn
            .execute(
                "INSERT INTO blocks (key, val) VALUES (?1, ?2)",
                (&key_bytes, &val_bytes),
            )
            .unwrap();
    }
    fn get(&self, c: Cid) -> Option<MPB> {
        let key_bytes = c.to_bytes();

        let val_bytes: Vec<u8> = self
            .conn
            .query_one(
                "SELECT val FROM blocks WHERE key = ?1",
                (&key_bytes,),
                |row| row.get(0),
            )
            .optional()
            .unwrap()?;

        let (t, n): (MPB, usize) =
            bincode::serde::decode_from_slice(&val_bytes, bincode::config::standard()).unwrap();
        assert_eq!(val_bytes.len(), n);
        Some(t)
    }
}
