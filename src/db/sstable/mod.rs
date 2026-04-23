pub mod datablock;
pub mod filter;
pub mod iterator;
pub mod reader;
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
use std::sync::{Arc, Mutex};

pub const FORMAT_VERSION: u32 = 1;
pub const FOOTER_SIZE: u64 = 64;
pub const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFF;

#[derive(Debug)]
pub struct SSTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) path: PathBuf,
    pub(crate) reader: Arc<Mutex<BufReader<File>>>,
    pub(crate) index: BTreeMap<K, u64>,
    pub(crate) meta: TableMeta<K>,
    pub(crate) filter: FilterVariant,
    pub(crate) id: SSTableId,
    pub(crate) version: u32,
    pub(crate) filter_offset: u64,
    pub(crate) index_offset: u64,
    pub(crate) file_size: u64,
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
            filter_offset: self.filter_offset,
            index_offset: self.index_offset,
            file_size: self.file_size,
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
    pub fn set_cache(&mut self, cache: Arc<crate::db::cache::BlockCache<K, V>>) {
        self.block_cache = Some(cache);
    }

    pub fn filter(&self) -> &FilterVariant {
        &self.filter
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn id(&self) -> SSTableId {
        self.id
    }
    pub fn len(&self) -> usize {
        self.meta.num_entries as usize
    }
    pub fn file_size(&self) -> u64 {
        self.file_size
    }
    pub fn min_key(&self) -> &K {
        &self.meta.min_key
    }
    pub fn max_key(&self) -> &K {
        &self.meta.max_key
    }
    pub fn num_entries(&self) -> u64 {
        self.meta.num_entries
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.overlaps_range(other.min_key(), other.max_key())
    }

    pub fn overlaps_range(&self, min: &K, max: &K) -> bool {
        self.min_key() <= max && min <= self.max_key()
    }

    pub fn iter(&self) -> Result<SSTableIterator<K, V>> {
        let file = File::open(&self.path)?;
        Ok(SSTableIterator::new(
            BufReader::new(file),
            self.filter_offset,
        ))
    }
}
