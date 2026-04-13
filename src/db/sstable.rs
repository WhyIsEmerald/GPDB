use crate::db::datablock::{BLOCK_SIZE, DataBlock, DeltaBlockBuilder};
use crate::db::io::{read_record, write_record};
use crate::{
    DBKey, Entry, Error, MemTable, Result, SSTableId, TableMeta, ValueEntry, COMPRESSION_NONE,
    FILTER_TYPE_XOR16, FILTER_TYPE_XOR8,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use xorf::{Filter, Xor8, Xor16};

const FORMAT_VERSION: u32 = 1;

/// Footer size: 64 bytes.
/// Layout: [filter_off: 8][index_off: 8][meta_off: 8][id: 8][magic: 8][version: 4][reserved: 20]
const FOOTER_SIZE: u64 = 64;
const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFF;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FilterVariant {
    Xor8(Xor8),
    Xor16(Xor16),
}

impl FilterVariant {
    pub fn contains(&self, key: &u64) -> bool {
        match self {
            Self::Xor8(f) => f.contains(key),
            Self::Xor16(f) => f.contains(key),
        }
    }
}

/// A Sorted String Table (SSTable) is an immutable, on-disk map from keys to values.
/// It uses a tiered XOR Filter (Xor8 for L0, Xor16 for L1+) for elite-speed lookups.
#[derive(Debug)]
pub struct SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
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
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
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
            _phantom: PhantomData,
        }
    }
}

impl<K, V> SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    /// Opens an existing SSTable file and loads its index, metadata, and XOR Filter.
    pub fn open(path: &Path) -> Result<Self> {
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

        // Load Metadata first to get filter type
        reader.seek(SeekFrom::Start(meta_offset))?;
        let meta: TableMeta<K> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable meta block is missing or empty".to_string())
        })?;

        // Load Filter based on type in meta
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

        // Load Sparse Index
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
            _phantom: PhantomData,
        })
    }

    /// Retrieves a value by first checking the XOR Filter to potentially skip disk I/O.
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

        let mut reader = self
            .reader
            .lock()
            .map_err(|_| Error::Corruption("Lock poisoned".to_string()))?;
        reader.seek(SeekFrom::Start(block_offset))?;

        let block: DataBlock<K, V> = read_record(&mut *reader)?
            .ok_or_else(|| Error::Corruption("Data block is missing".to_string()))?;

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

    /// Writes a stream of entries, using level to decide between Xor8 and Xor16.
    pub fn write_from_iter<I>(path: &Path, iter: I, id: SSTableId, level: usize) -> Result<Self>
    where
        K: DBKey,
        V: Serialize + DeserializeOwned,
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

        // 1. Build and Write XOR Filter
        let filter_offset = current_offset;
        let filter_type = if level > 0 { FILTER_TYPE_XOR16 } else { FILTER_TYPE_XOR8 };
        
        let filter_size = if filter_type == FILTER_TYPE_XOR16 {
            let f = Xor16::from(&key_hashes);
            write_record(&mut writer, &f)?
        } else {
            let f = Xor8::from(&key_hashes);
            write_record(&mut writer, &f)?
        };

        // 2. Write Sparse Index
        let index_offset = filter_offset + filter_size;
        let index_size = write_record(&mut writer, &sparse_index)?;

        // 3. Write Metadata
        let meta_offset = index_offset + index_size;
        let meta = TableMeta {
            min_key,
            max_key,
            num_entries,
            filter_type,
            compression_type: COMPRESSION_NONE,
        };
        write_record(&mut writer, &meta)?;

        // 4. Write Aligned Footer (64 bytes)
        writer.write_all(&filter_offset.to_le_bytes())?;
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&meta_offset.to_le_bytes())?;
        writer.write_all(&id.0.to_le_bytes())?;
        writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;
        writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
        writer.write_all(&[0u8; 20])?; // Reserved for future offsets/flags

        writer.flush()?;

        Self::open(path)
    }

    pub fn write_from_memtable(
        path: &Path,
        memtable: &MemTable<K, V>,
        id: SSTableId,
    ) -> Result<Self> {
        let iter = memtable.iter().map(|(k, v)| {
            Ok(Entry {
                key: k.clone(),
                value: v.clone(),
            })
        });
        Self::write_from_iter(path, iter, id, 0)
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
        Ok(SSTableIterator {
            reader: BufReader::new(file),
            data_end_offset: self.filter_offset,
            current_block: None,
            current_idx: 0,
            _phantom: PhantomData,
        })
    }
}

pub struct SSTableIterator<K, V> {
    reader: BufReader<File>,
    data_end_offset: u64,
    current_block: Option<DataBlock<K, V>>,
    current_idx: usize,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> SSTableIterator<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    fn load_next_block(&mut self) -> Result<bool> {
        let current_pos = self.reader.stream_position()?;
        if current_pos >= self.data_end_offset {
            return Ok(false);
        }

        match read_record(&mut self.reader)? {
            Some(block) => {
                self.current_block = Some(block);
                self.current_idx = 0;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl<K, V> Iterator for SSTableIterator<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(block) = &self.current_block {
                if let Some(entry) = block.iter().nth(self.current_idx) {
                    self.current_idx += 1;
                    return Some(Ok(entry));
                }
            }

            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup() -> (TempDir, PathBuf) {
        let tmp_dir = TempDir::new().expect("Failed to create temporary directory");
        let sstable_path = tmp_dir.path().join("L0-00001.sst");
        (tmp_dir, sstable_path)
    }

    #[test]
    fn test_tiered_xor_filters() {
        let (tmp_dir, _) = setup();
        
        // 1. Test L0 (Xor8)
        let l0_path = tmp_dir.path().join("L0.sst");
        let mut memtable = MemTable::new();
        memtable.put("k1".to_string(), Arc::new("v1".to_string()));
        let sst_l0 = SSTable::write_from_memtable(&l0_path, &memtable, SSTableId(1)).unwrap();
        assert!(matches!(sst_l0.filter, FilterVariant::Xor8(_)));
        assert!(sst_l0.get(&"k1".to_string()).unwrap().is_some());

        // 2. Test L1 (Xor16)
        let l1_path = tmp_dir.path().join("L1.sst");
        let entries = vec![Ok(Entry { key: "k2".to_string(), value: ValueEntry { value: Some(Arc::new("v2".to_string())), is_tombstone: false } })];
        let sst_l1 = SSTable::write_from_iter(&l1_path, entries.into_iter(), SSTableId(2), 1).unwrap();
        assert!(matches!(sst_l1.filter, FilterVariant::Xor16(_)));
        assert!(sst_l1.get(&"k2".to_string()).unwrap().is_some());
    }

    #[test]
    fn test_delta_encoding_correctness() {
        let (_tmp_dir, sstable_path) = setup();
        let mut memtable: MemTable<String, String> = MemTable::new();

        memtable.put("user_id_00001".to_string(), Arc::new("val1".to_string()));
        memtable.put("user_id_00002".to_string(), Arc::new("val2".to_string()));
        memtable.put("user_id_00003".to_string(), Arc::new("val3".to_string()));

        let sst = SSTable::write_from_memtable(&sstable_path, &memtable, SSTableId(1)).unwrap();

        assert_eq!(
            sst.get(&"user_id_00001".to_string())
                .unwrap()
                .unwrap()
                .value
                .unwrap()
                .as_str(),
            "val1"
        );
        assert_eq!(
            sst.get(&"user_id_00002".to_string())
                .unwrap()
                .unwrap()
                .value
                .unwrap()
                .as_str(),
            "val2"
        );
        assert_eq!(
            sst.get(&"user_id_00003".to_string())
                .unwrap()
                .unwrap()
                .value
                .unwrap()
                .as_str(),
            "val3"
        );

        let entries: Vec<_> = sst.iter().unwrap().map(|r| r.unwrap().key).collect();
        assert_eq!(
            entries,
            vec!["user_id_00001", "user_id_00002", "user_id_00003"]
        );
    }
}
