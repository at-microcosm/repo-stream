use crate::disk_drive::BlockStore;
use ipld_core::cid::Cid;
use redb::{Database, Error, ReadableDatabase, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("blocks");

pub struct RedbStore {
    db: Database,
}

impl RedbStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        log::warn!("redb new");
        let db = Database::create(path)?;
        log::warn!("db created");
        Ok(Self { db })
    }
}

// TODO: clean up on drop

impl<MPB: Serialize + DeserializeOwned> BlockStore<MPB> for RedbStore {
    fn put(&self, c: Cid, t: MPB) {
        let key_bytes = c.to_bytes();
        let val_bytes = bincode::serde::encode_to_vec(t, bincode::config::standard()).unwrap();

        let mut tx = self.db.begin_write().unwrap();
        tx.set_durability(redb::Durability::None).unwrap();
        {
            let mut table = tx.open_table(TABLE).unwrap();
            table.insert(&*key_bytes, &*val_bytes).unwrap();
        }
        tx.commit().unwrap();
    }
    fn get(&self, c: Cid) -> Option<MPB> {
        let key_bytes = c.to_bytes();
        let tx = self.db.begin_read().unwrap();
        let table = tx.open_table(TABLE).unwrap();
        let maybe_val_bytes = table.get(&*key_bytes).unwrap()?;
        let (t, n): (MPB, usize) =
            bincode::serde::decode_from_slice(maybe_val_bytes.value(), bincode::config::standard())
                .unwrap();
        assert_eq!(maybe_val_bytes.value().len(), n);
        Some(t)
    }
}
