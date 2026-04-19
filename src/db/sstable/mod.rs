pub mod datablock;
pub mod filter;
pub mod iterator;

pub use datablock::*;
pub use filter::*;
pub use iterator::*;

use crate::db::io::{read_record, write_record};
use crate::{
    COMPRESSION_NONE, DBKey, Entry, Error, FILTER_TYPE_XOR8, FILTER_TYPE_XOR16, MemTable, Result,
    SSTableId, TableMeta, ValueEntry,
};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use xorf::{Xor8, Xor16};

pub const FORMAT_VERSION: u32 = 1;
pub const FOOTER_SIZE: u64 = 64;
pub const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFF;

#[derive(Debug)]
pub struct SSTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    path: PathBuf,
    reader: Arc<Mutex<BufReader<File>>>,
    index: BTreeMap<K, u64>,
    meta: TableMeta<K>,
    filter: FilterVariant,
    id: SSTableId,
    version: u32,
    filter_offset: u64,
    index_offset: u64,
    file_size: u64,
    block_cache: Option<Arc<crate::db::cache::BlockCache<K, V>>>,
    _phantom: PhantomData<(K, V)>,
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
    pub fn open(
        path: &Path,
        block_cache: Option<Arc<crate::db::cache::BlockCache<K, V>>>,
    ) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let file_len = reader.seek(SeekFrom::End(0))?;
        if file_len < FOOTER_SIZE {
            return Err(Error::Corruption("SSTable too small".to_string()));
        }

        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let mut buf_8 = [0u8; 8];
        let mut buf_4 = [0u8; 4];

        reader.read_exact(&mut buf_8)?;
        let filter_offset = u64::from_le_bytes(buf_8);

        reader.read_exact(&mut buf_8)?;
        let index_offset = u64::from_le_bytes(buf_8);

        reader.read_exact(&mut buf_8)?;
        let meta_offset = u64::from_le_bytes(buf_8);

        reader.read_exact(&mut buf_8)?;
        let id = SSTableId(u64::from_le_bytes(buf_8));

        reader.read_exact(&mut buf_8)?;
        let magic_number = u64::from_le_bytes(buf_8);

        reader.read_exact(&mut buf_4)?;
        let version = u32::from_le_bytes(buf_4);

        if magic_number != MAGIC_NUMBER {
            return Err(Error::Corruption("Invalid magic number".to_string()));
        }
        if version > FORMAT_VERSION {
            return Err(Error::Corruption(format!(
                "Unsupported SSTable version: {}",
                version
            )));
        }

        reader.seek(SeekFrom::Start(meta_offset))?;
        let meta: TableMeta<K> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable meta block is missing or empty".to_string())
        })?;

        reader.seek(SeekFrom::Start(filter_offset))?;
        let filter: FilterVariant = if meta.filter_type == FILTER_TYPE_XOR16 {
            let f: Xor16 = read_record(&mut reader)?.ok_or_else(|| {
                Error::Corruption("SSTable Xor16 filter block is missing".to_string())
            })?;
            FilterVariant::Xor16(f)
        } else {
            let f: Xor8 = read_record(&mut reader)?.ok_or_else(|| {
                Error::Corruption("SSTable Xor8 filter block is missing".to_string())
            })?;
            FilterVariant::Xor8(f)
        };

        reader.seek(SeekFrom::Start(index_offset))?;
        let index: BTreeMap<K, u64> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable index block is missing or empty".to_string())
        })?;

        Ok(SSTable {
            path: path.to_path_buf(),
            reader: Arc::new(Mutex::new(reader)),
            index,
            meta,
            filter,
            id,
            version,
            filter_offset,
            index_offset,
            file_size: file_len,
            block_cache,
            _phantom: PhantomData,
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<ValueEntry<V>>> {
        if key < self.min_key() || key > self.max_key() {
            return Ok(None);
        }

        let key_hash = self.hash_key(key);
        if !self.filter.contains(&key_hash) {
            return Ok(None);
        }

        let block_offset = match self.index.range(..=key.clone()).next_back() {
            Some((_, offset)) => *offset,
            None => return Ok(None),
        };

        let block = if let Some(cache) = &self.block_cache {
            if let Some(cached_block) = cache.get(self.id, block_offset) {
                cached_block
            } else {
                let mut reader = self
                    .reader
                    .lock()
                    .map_err(|_| Error::Corruption("Lock poisoned".to_string()))?;
                reader.seek(SeekFrom::Start(block_offset))?;

                let block: DataBlock<K, V> = read_record(&mut *reader)?
                    .ok_or_else(|| Error::Corruption("Data block is missing".to_string()))?;
                let arc_block = Arc::new(block);
                cache.insert(self.id, block_offset, Arc::clone(&arc_block));
                arc_block
            }
        } else {
            let mut reader = self
                .reader
                .lock()
                .map_err(|_| Error::Corruption("Lock poisoned".to_string()))?;
            reader.seek(SeekFrom::Start(block_offset))?;

            let block: DataBlock<K, V> = read_record(&mut *reader)?
                .ok_or_else(|| Error::Corruption("Data block is missing".to_string()))?;
            Arc::new(block)
        };

        for entry in block.iter() {
            if &entry.key == key {
                return Ok(Some(entry.value));
            }
            if &entry.key > key {
                break;
            }
        }

        Ok(None)
    }

    fn hash_key(&self, key: &K) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    pub fn write_from_iter<I>(
        path: &Path,
        iter: I,
        id: SSTableId,
        level: usize,
        block_cache: Option<Arc<crate::db::cache::BlockCache<K, V>>>,
    ) -> Result<Self>
    where
        K: DBKey,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let mut sparse_index = BTreeMap::new();
        let mut key_hashes = Vec::new();

        let mut min_key = None;
        let mut max_key = None;
        let mut num_entries = 0;

        let mut current_offset = 0;
        let mut builder = DeltaBlockBuilder::new(BLOCK_SIZE);

        for item in iter {
            let entry = item?;
            if min_key.is_none() {
                min_key = Some(entry.key.clone());
            }
            max_key = Some(entry.key.clone());
            num_entries += 1;

            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hasher;
            let mut s = DefaultHasher::new();
            entry.key.hash(&mut s);
            key_hashes.push(s.finish());

            if builder.is_empty() {
                sparse_index.insert(entry.key.clone(), current_offset);
            }

            builder.add(&entry.key, &entry.value);

            if builder.is_full() {
                let block = builder.finish();
                let bytes_written = write_record(&mut writer, &block)?;
                current_offset += bytes_written;
            }
        }

        if !builder.is_empty() {
            let block = builder.finish();
            let bytes_written = write_record(&mut writer, &block)?;
            current_offset += bytes_written;
        }

        let min_key = min_key.ok_or_else(|| Error::Corruption("Empty SSTable".to_string()))?;
        let max_key = max_key.unwrap();

        let filter_offset = current_offset;
        let filter_type = if level > 0 {
            FILTER_TYPE_XOR16
        } else {
            FILTER_TYPE_XOR8
        };

        let filter_size = if filter_type == FILTER_TYPE_XOR16 {
            let f = Xor16::from(&key_hashes);
            write_record(&mut writer, &f)?
        } else {
            let f = Xor8::from(&key_hashes);
            write_record(&mut writer, &f)?
        };

        let index_offset = filter_offset + filter_size;
        let index_size = write_record(&mut writer, &sparse_index)?;

        let meta_offset = index_offset + index_size;
        let meta = TableMeta {
            min_key,
            max_key,
            num_entries,
            filter_type,
            compression_type: COMPRESSION_NONE,
        };
        write_record(&mut writer, &meta)?;

        writer.write_all(&filter_offset.to_le_bytes())?;
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&meta_offset.to_le_bytes())?;
        writer.write_all(&id.0.to_le_bytes())?;
        writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;
        writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
        writer.write_all(&[0u8; 20])?;

        writer.flush()?;

        Self::open(path, block_cache)
    }

    pub fn write_from_memtable(
        path: &Path,
        memtable: &MemTable<K, V>,
        id: SSTableId,
        block_cache: Option<Arc<crate::db::cache::BlockCache<K, V>>>,
    ) -> Result<Self> {
        let iter = memtable.iter().map(|(k, v)| {
            Ok(Entry {
                key: k.clone(),
                value: v.clone(),
            })
        });
        Self::write_from_iter(path, iter, id, 0, block_cache)
    }

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
