/// Data block implementation.
pub mod datablock;
/// Filter implementation.
pub mod filter;
/// Iterator implementation.
pub mod iterator;
/// Reader implementation.
pub mod reader;
/// Writer implementation.
pub mod writer;

pub use datablock::*;
pub use filter::FilterVariant;
pub use iterator::*;

use crate::DBKey;
pub use crate::types::sstable::{FILTER_TYPE_XOR8, FILTER_TYPE_XOR16};
use crate::{Result, SSTableId, TableMeta};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

/// The format version of the SSTable.
pub const FORMAT_VERSION: u32 = 1;
/// The size of the SSTable footer.
pub const FOOTER_SIZE: u64 = 64;
/// The magic number used to identify SSTable files.
pub const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFF;

/// A struct that represents an on-disk sorted string table (SSTable).
#[derive(Debug)]
pub struct SSTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The path used for the SSTable file on disk.
    pub(crate) path: PathBuf,
    /// The reader used for accessing the SSTable file.
    pub(crate) reader: Arc<Mutex<BufReader<File>>>,
    /// The index used for mapping keys to offsets.
    pub(crate) index: BTreeMap<Arc<K>, u64>,
    /// The metadata used for the SSTable.
    pub(crate) meta: TableMeta<K>,
    /// The filter used for the SSTable.
    pub(crate) filter: FilterVariant,
    /// The unique identifier used for the SSTable.
    pub(crate) id: SSTableId,
    /// The version used for the SSTable.
    pub(crate) version: u32,
    /// The minimum sequence number used in the SSTable.
    pub(crate) min_seq: u64,
    /// The maximum sequence number used in the SSTable.
    pub(crate) max_seq: u64,
    /// The offset used for the filter.
    pub(crate) filter_offset: u64,
    /// The offset used for the index.
    pub(crate) index_offset: u64,
    /// The total size of the SSTable file.
    pub(crate) file_size: u64,
    /// The number of filter hits.
    pub(crate) filter_hits: Arc<AtomicU64>,
    /// The number of filter misses.
    pub(crate) filter_misses: Arc<AtomicU64>,
    /// The block cache used for the SSTable.
    pub(crate) block_cache: Option<Arc<crate::db::cache::BlockCache<K, V>>>,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for SSTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            reader: Arc::clone(&self.reader),
            index: self.index.clone(),
            meta: self.meta.clone(),
            filter: self.filter.clone(),
            id: self.id,
            version: self.version,
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            filter_offset: self.filter_offset,
            index_offset: self.index_offset,
            file_size: self.file_size,
            filter_hits: Arc::clone(&self.filter_hits),
            filter_misses: Arc::clone(&self.filter_misses),
            block_cache: self.block_cache.as_ref().map(Arc::clone),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> SSTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Assigns a block cache to the SSTable.
    pub fn set_cache(&mut self, cache: Arc<crate::db::cache::BlockCache<K, V>>) {
        self.block_cache = Some(cache);
    }

    /// Returns the filter used for the SSTable.
    pub fn filter(&self) -> &FilterVariant {
        &self.filter
    }

    /// Returns the filter hit and miss statistics.
    pub fn filter_stats(&self) -> (u64, u64) {
        (
            self.filter_hits.load(Ordering::Relaxed),
            self.filter_misses.load(Ordering::Relaxed),
        )
    }

    /// Returns the path used for the SSTable.
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Returns the unique identifier of the SSTable.
    pub fn id(&self) -> SSTableId {
        self.id
    }
    /// Returns the number of entries in the SSTable.
    pub fn len(&self) -> usize {
        self.meta.num_entries as usize
    }

    /// Checks if the SSTable is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns the total size of the SSTable file.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }
    /// Returns the smallest key in the SSTable.
    pub fn min_key(&self) -> &K {
        &self.meta.min_key
    }
    /// Returns the largest key in the SSTable.
    pub fn max_key(&self) -> &K {
        &self.meta.max_key
    }
    /// Returns the number of entries in the SSTable.
    pub fn num_entries(&self) -> u64 {
        self.meta.num_entries
    }

    /// Checks if the SSTable overlaps with another SSTable.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.overlaps_range(other.min_key(), other.max_key())
    }

    /// Checks if the SSTable overlaps with a given key range.
    pub fn overlaps_range(&self, min: &K, max: &K) -> bool {
        self.min_key() <= max && min <= self.max_key()
    }

    /// Creates a sorted iterator over the SSTable.
    pub fn iter(&self) -> Result<SSTableIterator<K, V>> {
        let file = File::open(&self.path)?;
        Ok(SSTableIterator::new(
            BufReader::new(file),
            self.filter_offset,
            self.meta.compression_type,
        ))
    }
}
