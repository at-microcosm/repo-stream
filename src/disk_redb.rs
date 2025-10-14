use crate::disk_drive::BlockStore;
use ipld_core::cid::Cid;
use redb::{Database, Durability, Error, ReadableDatabase, TableDefinition};
use std::path::Path;
use std::sync::Arc;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("blocks");

pub struct RedbStore {
    #[allow(dead_code)]
    db: Arc<Database>,
}

impl RedbStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        log::warn!("redb new");
        let db = Database::create(path)?;
        log::warn!("db created");
        Ok(Self { db: db.into() })
    }
}

impl Drop for RedbStore {
    fn drop(&mut self) {
        let mut tx = self.db.begin_write().unwrap();
        tx.set_durability(Durability::None).unwrap();
        tx.delete_table(TABLE).unwrap();
        tx.commit().unwrap();
    }
}

impl BlockStore<Vec<u8>> for RedbStore {
    async fn put_batch(&self, blocks: Vec<(Cid, Vec<u8>)>) {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = db.begin_write().unwrap();
            tx.set_durability(Durability::None).unwrap();

            {
                let mut table = tx.open_table(TABLE).unwrap();
                for (cid, t) in blocks {
                    let key_bytes = cid.to_bytes();
                    table.insert(&*key_bytes, &*t).unwrap();
                }
            }

            tx.commit().unwrap();
        }).await.unwrap();
    }

    fn get(&self, c: Cid) -> Option<Vec<u8>> {
        let key_bytes = c.to_bytes();
        let tx = self.db.begin_read().unwrap();
        let table = tx.open_table(TABLE).unwrap();
        let t = table.get(&*key_bytes).unwrap()?.value().to_vec();
        Some(t)
    }
}
