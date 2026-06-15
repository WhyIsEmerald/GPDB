//! Block cache implementation.
//!
//! Provides a mechanism to cache de-serialized data blocks from SSTables to reduce disk I/O.

use crate::db::sstable::datablock::DataBlock;
use crate::{DBKey, SSTableId};
use moka::sync::Cache;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A struct that represents a thread-safe cache for de-serialized SSTable data blocks.
pub struct BlockCache<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The cache used for mapping SSTable ID and offset to a data block.
    cache: Cache<(SSTableId, u64), Arc<DataBlock<K, V>>>,
    /// The hit count used for successful cache lookups.
    pub(crate) hits: AtomicU64,
    /// The miss count used for failed cache lookups.
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
    /// Creates a new block cache with the specified capacity in bytes.
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

    /// Retrieves a data block from the cache.
    pub fn get(&self, sstable_id: SSTableId, offset: u64) -> Option<Arc<DataBlock<K, V>>> {
        let res = self.cache.get(&(sstable_id, offset));
        if res.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    /// Inserts a data block into the cache.
    pub fn insert(&self, sstable_id: SSTableId, offset: u64, block: Arc<DataBlock<K, V>>) {
        self.cache.insert((sstable_id, offset), block);
    }
}
