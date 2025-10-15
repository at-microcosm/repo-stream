use crate::disk_drive::BlockStore;
use crate::disk_walk::{Need, Walker};
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
    pub async fn new(path: impl AsRef<Path> + 'static + Send) -> Result<Self, Error> {
        log::warn!("redb new");
        let db = tokio::task::spawn_blocking(|| Database::create(path))
            .await
            .unwrap()?;
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
        })
        .await
        .unwrap();
    }

    async fn walk_batch(
        &self,
        mut walker: Walker,
        n: usize,
    ) -> Result<(Walker, Vec<(String, Vec<u8>)>), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let tx = db.begin_read().unwrap();
            let table = tx.open_table(TABLE).unwrap();

            let mut out = Vec::with_capacity(n);
            loop {
                let Some(need) = walker.next_needed() else {
                    break;
                };
                let cid = need.cid();
                let Some(res) = table.get(&*cid.to_bytes()).unwrap() else {
                    return Err(format!("missing block: {cid:?}"));
                };
                let block = res.value();

                match need {
                    Need::Node(_) => walker
                        .handle_node(block)
                        .map_err(|e| format!("failed to handle mst node: {e}"))?,
                    Need::Record { rkey, .. } => {
                        out.push((rkey, block.to_vec()));
                        if out.len() >= n {
                            break;
                        }
                    }
                }
            }
            Ok((walker, out))
        })
        .await
        .unwrap() // tokio join
    }
}
