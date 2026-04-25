use crate::db::datablock::DeltaBlockBuilder;
use crate::db::io::write_record;
use crate::db::sstable::datablock::BLOCK_SIZE;
use crate::db::sstable::{
    FILTER_TYPE_XOR8, FILTER_TYPE_XOR16, FORMAT_VERSION, MAGIC_NUMBER, SSTable,
};
use crate::{COMPRESSION_NONE, DBKey, Entry, Error, MemTable, Result, SSTableId, TableMeta};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use xorf::{Xor8, Xor16};

impl<K, V> SSTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
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
                min_key = Some(Arc::clone(&entry.key));
            }
            max_key = Some(Arc::clone(&entry.key));
            num_entries += 1;

            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hasher;
            let mut s = DefaultHasher::new();
            entry.key.hash(&mut s);
            key_hashes.push(s.finish());

            if builder.is_empty() {
                sparse_index.insert(Arc::clone(&entry.key), current_offset);
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

        let filter_offset: u64 = current_offset;
        let filter_type = if level > 0 {
            FILTER_TYPE_XOR16
        } else {
            FILTER_TYPE_XOR8
        };

        let filter_size: u64 = if filter_type == FILTER_TYPE_XOR16 {
            let f = Xor16::from(&key_hashes);
            write_record(&mut writer, &f)?
        } else {
            let f = Xor8::from(&key_hashes);
            write_record(&mut writer, &f)?
        };

        let index_offset: u64 = filter_offset + filter_size;
        let index_size: u64 = write_record(&mut writer, &sparse_index)?;

        let meta_offset: u64 = index_offset + index_size;
        let meta = TableMeta {
            min_key: (*min_key).clone(),
            max_key: (*max_key).clone(),
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
        let iter = memtable.iter().map(|(k, v)| Ok(Entry { key: k, value: v }));
        Self::write_from_iter(path, iter, id, 0, block_cache)
    }
}
