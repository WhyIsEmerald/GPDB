use crate::db::sstable::datablock::DataBlock;
use crate::{DBKey, SSTableId};
use moka::sync::Cache;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A thread-safe block cache using Moka (W-TinyLFU).
/// Caches de-serialized DataBlocks to skip disk I/O and CPU overhead of parsing.
pub struct BlockCache<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    cache: Cache<(SSTableId, u64), Arc<DataBlock<K, V>>>,
    pub(crate) hits: AtomicU64,
    pub(crate) misses: AtomicU64,
}

impl<K, V> std::fmt::Debug for BlockCache<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("entry_count", &self.cache.entry_count())
            .finish()
    }
}

impl<K, V> BlockCache<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity_bytes)
            .weigher(|_key, block: &Arc<DataBlock<K, V>>| block.data.len() as u32)
            .build();

        Self {
            cache,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, sstable_id: SSTableId, offset: u64) -> Option<Arc<DataBlock<K, V>>> {
        let res = self.cache.get(&(sstable_id, offset));
        if res.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    pub fn insert(&self, sstable_id: SSTableId, offset: u64, block: Arc<DataBlock<K, V>>) {
        self.cache.insert((sstable_id, offset), block);
    }
}
