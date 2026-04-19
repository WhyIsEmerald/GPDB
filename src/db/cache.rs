use crate::db::sstable::datablock::DataBlock;
use crate::{DBKey, SSTableId};
use moka::sync::Cache;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// A thread-safe block cache using Moka (W-TinyLFU).
/// Caches de-serialized DataBlocks to skip disk I/O and CPU overhead of parsing.
pub struct BlockCache<K, V> 
where 
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    cache: Cache<(SSTableId, u64), Arc<DataBlock<K, V>>>,
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
        // Moka can bound by entry count or weight. 
        // For now, we use entry count assuming ~4KB blocks.
        // 10,000 blocks ~= 40MB.
        let max_capacity = capacity_bytes / 4096;
        
        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .build();
            
        Self { cache }
    }

    pub fn get(&self, sstable_id: SSTableId, offset: u64) -> Option<Arc<DataBlock<K, V>>> {
        self.cache.get(&(sstable_id, offset))
    }

    pub fn insert(&self, sstable_id: SSTableId, offset: u64, block: Arc<DataBlock<K, V>>) {
        self.cache.insert((sstable_id, offset), block);
    }
}
