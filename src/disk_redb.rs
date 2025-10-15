use crate::disk_drive::{BlockStore, BlockStoreError};
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

// TODO: ship off to a blocking thread
impl Drop for RedbStore {
    fn drop(&mut self) {
        let mut tx = self.db.begin_write().unwrap();
        tx.set_durability(Durability::None).unwrap();
        tx.delete_table(TABLE).unwrap();
        tx.commit().unwrap();
    }
}

impl<E: Into<Error>> From<E> for BlockStoreError {
    fn from(e: E) -> BlockStoreError {
        let e = Into::<Error>::into(e);
        BlockStoreError::StorageBackend(Box::new(e))
    }
}

impl BlockStore<Vec<u8>> for RedbStore {
    async fn put_batch(&self, blocks: Vec<(Cid, Vec<u8>)>) -> Result<(), BlockStoreError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> Result<(), BlockStoreError> {
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::None)?;

            {
                let mut table = tx.open_table(TABLE)?;
                for (cid, t) in blocks {
                    let key_bytes = cid.to_bytes();
                    table.insert(&*key_bytes, &*t)?;
                }
            }

            Ok(tx.commit()?)
        })
        .await
        .map_err(BlockStoreError::JoinError)?
    }

    async fn walk_batch(
        &self,
        mut walker: Walker,
        n: usize,
    ) -> Result<(Walker, Vec<(String, Vec<u8>)>), BlockStoreError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> Result<_, BlockStoreError> {
            let tx = db.begin_read()?;
            let table = tx.open_table(TABLE)?;

            let mut out = Vec::with_capacity(n);
            loop {
                let Some(need) = walker.next_needed()? else {
                    break;
                };
                let cid = need.cid();
                let Some(res) = table.get(&*cid.to_bytes())? else {
                    return Err(BlockStoreError::MissingBlock(cid));
                };
                let block = res.value();

                match need {
                    Need::Node(_) => walker
                        .handle_node(block)?,
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
        .map_err(BlockStoreError::JoinError)?
    }
}
