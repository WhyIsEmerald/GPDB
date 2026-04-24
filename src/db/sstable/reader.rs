use crate::db::io::read_record;
use crate::db::sstable::datablock::DataBlock;
use crate::db::sstable::{
    FILTER_TYPE_XOR16, FOOTER_SIZE, FORMAT_VERSION, FilterVariant, MAGIC_NUMBER, SSTable,
};
use crate::{DBKey, Error, Result, SSTableId, TableMeta, ValueEntry};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};
use xorf::{Xor8, Xor16};

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
        let index_raw: BTreeMap<K, u64> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable index block is missing or empty".to_string())
        })?;
        let index = index_raw.into_iter().map(|(k, v)| (Arc::new(k), v)).collect();

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

        block.get(key).map_or(Ok(None), |v| Ok(Some(v)))
    }

    pub(crate) fn hash_key(&self, key: &K) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }
}
