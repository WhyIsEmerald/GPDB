use crate::db::datablock::{BLOCK_SIZE, BlockBuilder, DataBlock};
use crate::db::io::{read_record, write_record};
use crate::{DBKey, Entry, Error, MemTable, Result, SSTableId, TableMeta, ValueEntry};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Current version of the SSTable format.
const FORMAT_VERSION: u32 = 1;
/// No compression for now.
const COMPRESSION_NONE: u8 = 0;

/// The fixed size of the footer (48 bytes)
/// Contains (index_off: 8, meta_off: 8, id: 8, magic: 8, version: 4, compression: 1, padding: 11)
/// All 8-byte values are aligned to 8-byte boundaries.
const FOOTER_SIZE: u64 = 48;
const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFF;

/// A Sorted String Table (SSTable) is an immutable, on-disk map from keys to values.
/// It uses a Sparse Index for fast lookups and metadata for range-based overlap checks.
#[derive(Debug)]
pub struct SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    path: PathBuf,
    // Arc allows multiple handles to the same underlying file and reader
    // Mutex allows interior mutability so we can seek/read while having a &self reference
    reader: Arc<Mutex<BufReader<File>>>,
    /// Sparse Index: Maps the FIRST key of a block to its disk offset.
    index: BTreeMap<K, u64>,
    meta: TableMeta<K>,
    id: SSTableId,
    version: u32,
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
            id: self.id,
            version: self.version,
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
    /// Opens an existing SSTable file from disk and loads its index and metadata into memory.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let file_len = reader.seek(SeekFrom::End(0))?;
        if file_len < FOOTER_SIZE {
            return Err(Error::Corruption(
                "SSTable is too small to contain a valid footer".to_string(),
            ));
        }
        
        // Seek to the start of the aligned footer
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        
        let mut buf_8 = [0u8; 8];
        let mut buf_4 = [0u8; 4];
        let mut buf_1 = [0u8; 1];

        // 1. Read Aligned 8-byte Offsets
        reader.read_exact(&mut buf_8)?;
        let index_offset = u64::from_le_bytes(buf_8);
        
        reader.read_exact(&mut buf_8)?;
        let meta_offset = u64::from_le_bytes(buf_8);
        
        // 2. Read Aligned 8-byte ID and Magic
        reader.read_exact(&mut buf_8)?;
        let id = SSTableId(u64::from_le_bytes(buf_8));

        reader.read_exact(&mut buf_8)?;
        let magic_number = u64::from_le_bytes(buf_8);

        // 3. Read Version and Compression
        reader.read_exact(&mut buf_4)?;
        let version = u32::from_le_bytes(buf_4);
        
        reader.read_exact(&mut buf_1)?;
        let _compression = buf_1[0];

        // Verification
        if magic_number != MAGIC_NUMBER {
            return Err(Error::Corruption("Invalid magic number".to_string()));
        }
        if version > FORMAT_VERSION {
            return Err(Error::Corruption(format!("Unsupported SSTable version: {}", version)));
        }

        // Load Sparse Index
        reader.seek(SeekFrom::Start(index_offset))?;
        let index: BTreeMap<K, u64> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable index block is missing or empty".to_string())
        })?;

        // Load Metadata
        reader.seek(SeekFrom::Start(meta_offset))?;
        let meta: TableMeta<K> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable meta block is missing or empty".to_string())
        })?;

        Ok(SSTable {
            path: path.to_path_buf(),
            reader: Arc::new(Mutex::new(reader)),
            index,
            meta,
            id,
            version,
            index_offset,
            file_size: file_len,
            _phantom: PhantomData,
        })
    }

    /// Retrieves a value by finding the correct block and binary searching its restart points.
    pub fn get(&self, key: &K) -> Result<Option<ValueEntry<V>>> {
        // 1. Coarse Range Check
        if key < self.min_key() || key > self.max_key() {
            return Ok(None);
        }

        // 2. Sparse Index Binary Search
        let block_offset = match self.index.range(..=key.clone()).next_back() {
            Some((_, offset)) => *offset,
            None => return Ok(None),
        };

        // 3. Load exactly ONE 4KB block from disk
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| Error::Corruption("Lock poisoned".to_string()))?;
        reader.seek(SeekFrom::Start(block_offset))?;

        let block: DataBlock<K, V> = read_record(&mut *reader)?
            .ok_or_else(|| Error::Corruption("Data block is missing or corrupted".to_string()))?;

        // 4. Inner-Block Binary Search using Restart Points
        let restart_idx = block
            .restart_points
            .partition_point(|&idx| &block.entries[idx as usize].key <= key);

        let start_search_idx = if restart_idx > 0 {
            block.restart_points[restart_idx - 1] as usize
        } else {
            0
        };

        // 5. Linear scan from the restart point (at most RESTART_INTERVAL entries)
        for entry in &block.entries[start_search_idx..] {
            if &entry.key == key {
                return Ok(Some(entry.value.clone()));
            }
            if &entry.key > key {
                break;
            }
        }

        Ok(None)
    }

    /// Writes a stream of sorted entries to a new SSTable file in Blocked format.
    pub fn write_from_iter<I>(path: &Path, iter: I, id: SSTableId) -> Result<Self>
    where
        K: DBKey,
        V: Serialize + DeserializeOwned,
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let mut sparse_index = BTreeMap::new();

        let mut min_key = None;
        let mut max_key = None;
        let mut num_entries = 0;

        let mut current_offset = 0;
        let mut builder = BlockBuilder::new(BLOCK_SIZE);

        for item in iter {
            let entry = item?;
            if min_key.is_none() {
                min_key = Some(entry.key.clone());
            }
            max_key = Some(entry.key.clone());
            num_entries += 1;

            if builder.is_empty() {
                sparse_index.insert(entry.key.clone(), current_offset);
            }

            builder.add(entry);

            if builder.is_full() {
                let (entries, restart_points) = builder.finish();
                let block = DataBlock {
                    entries,
                    restart_points,
                };
                let bytes_written = write_record(&mut writer, &block)?;
                current_offset += bytes_written;
            }
        }

        if !builder.is_empty() {
            let (entries, restart_points) = builder.finish();
            let block = DataBlock {
                entries,
                restart_points,
            };
            let bytes_written = write_record(&mut writer, &block)?;
            current_offset += bytes_written;
        }

        let min_key = min_key
            .ok_or_else(|| Error::Corruption("Cannot write an empty SSTable".to_string()))?;
        let max_key = max_key.unwrap();

        // Write the index block
        let index_offset = current_offset;
        let index_size = write_record(&mut writer, &sparse_index)?;

        // Write the meta block
        let meta_offset = index_offset + index_size;
        let meta = TableMeta {
            min_key,
            max_key,
            num_entries,
        };
        let _meta_size = write_record(&mut writer, &meta)?;

        // Write the 48-byte Aligned Footer
        writer.write_all(&index_offset.to_le_bytes())?;    // 8
        writer.write_all(&meta_offset.to_le_bytes())?;     // 8
        writer.write_all(&id.0.to_le_bytes())?;            // 8
        writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;    // 8
        writer.write_all(&FORMAT_VERSION.to_le_bytes())?;  // 4
        writer.write_all(&[COMPRESSION_NONE])?;            // 1
        writer.write_all(&[0u8; 11])?;                     // 11 (Padding to 48 bytes)
        
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
        Self::write_from_iter(path, iter, id)
    }

    pub fn path(&self) -> &Path { &self.path }
    pub fn id(&self) -> SSTableId { self.id }
    pub fn len(&self) -> usize { self.meta.num_entries as usize }
    pub fn file_size(&self) -> u64 { self.file_size }
    pub fn min_key(&self) -> &K { &self.meta.min_key }
    pub fn max_key(&self) -> &K { &self.meta.max_key }
    pub fn num_entries(&self) -> u64 { self.meta.num_entries }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.overlaps_range(other.min_key(), other.max_key())
    }

    pub fn overlaps_range(&self, min: &K, max: &K) -> bool {
        self.min_key() <= max && min <= self.max_key()
    }

    /// Returns a sequential iterator over all entries in this SSTable.
    pub fn iter(&self) -> Result<SSTableIterator<K, V>> {
        let file = File::open(&self.path)?;
        Ok(SSTableIterator {
            reader: BufReader::new(file),
            index_offset: self.index_offset,
            current_block: None,
            current_idx: 0,
            _phantom: PhantomData,
        })
    }
}

pub struct SSTableIterator<K, V> {
    reader: BufReader<File>,
    index_offset: u64,
    current_block: Option<DataBlock<K, V>>,
    current_idx: usize,
    _phantom: PhantomData<(K, V)>,
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
                if self.current_idx < block.entries.len() {
                    let entry = block.entries[self.current_idx].clone();
                    self.current_idx += 1;
                    return Some(Ok(entry));
                }
            }

            let current_pos = self.reader.stream_position().ok()?;
            if current_pos >= self.index_offset {
                return None;
            }

            match read_record(&mut self.reader) {
                Ok(Some(block)) => {
                    self.current_block = Some(block);
                    self.current_idx = 0;
                }
                Ok(None) => return None,
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
    fn write_and_open() {
        let (_tmp_dir, sstable_path) = setup();
        let mut memtable: MemTable<String, String> = MemTable::new();
        memtable.put("key1".to_string(), Arc::new("value1".to_string()));
        memtable.put("key2".to_string(), Arc::new("value2".to_string()));
        let sstable: SSTable<String, String> =
            SSTable::write_from_memtable(&sstable_path, &memtable, SSTableId(1))
                .expect("Failed to write to SSTable");

        assert_eq!(sstable.len(), 2);
        assert_eq!(sstable.id(), SSTableId(1));

        let sstable: SSTable<String, String> =
            SSTable::open(&sstable_path).expect("Failed to open SSTable");
        assert_eq!(sstable.num_entries(), 2);
        assert_eq!(sstable.id(), SSTableId(1));
        assert!(sstable.index.contains_key("key1"));
        assert!(!sstable.index.contains_key("key2"));
    }

    #[test]
    fn get() {
        let (_tmp_dir, sstable_path) = setup();
        let mut memtable: MemTable<String, String> = MemTable::new();
        memtable.put("key1".to_string(), Arc::new("value1".to_string()));
        memtable.delete("key2".to_string());
        let sstable: SSTable<String, String> =
            SSTable::write_from_memtable(&sstable_path, &memtable, SSTableId(1))
                .expect("Failed to write to SSTable");

        assert_eq!(sstable.len(), 2);

        let sstable: SSTable<String, String> =
            SSTable::open(&sstable_path).expect("Failed to open SSTable");

        let value_entry1 = sstable
            .get(&"key1".to_string())
            .expect("Failed to get k1")
            .expect("k1 not found");
        assert_eq!(value_entry1.value.unwrap().as_ref(), &"value1".to_string());
        assert!(!value_entry1.is_tombstone);

        let value_entry2 = sstable
            .get(&"key2".to_string())
            .expect("Failed to get k2")
            .expect("k2 not found");
        assert!(value_entry2.is_tombstone);
        assert_eq!(value_entry2.value, None);

        let entry3 = sstable.get(&"key3".to_string()).expect("Failed to get k3");
        assert!(entry3.is_none());
    }

    #[test]
    fn iteration() {
        let (_tmp_dir, sstable_path) = setup();
        let mut memtable: MemTable<String, String> = MemTable::new();
        memtable.put("k1".to_string(), Arc::new("v1".to_string()));
        memtable.put("k2".to_string(), Arc::new("v2".to_string()));
        memtable.delete("k3".to_string());

        let sstable: SSTable<String, String> =
            SSTable::write_from_memtable(&sstable_path, &memtable, SSTableId(1)).unwrap();

        let mut iter = sstable.iter().unwrap();

        let e1 = iter.next().unwrap().unwrap();
        assert_eq!(e1.key, "k1");
        assert_eq!(e1.value.value.unwrap().as_str(), "v1");

        let e2 = iter.next().unwrap().unwrap();
        assert_eq!(e2.key, "k2");
        assert_eq!(e2.value.value.unwrap().as_str(), "v2");

        let e3 = iter.next().unwrap().unwrap();
        assert_eq!(e3.key, "k3");
        assert!(e3.value.is_tombstone);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_sparse_index_and_blocks() {
        let (_tmp_dir, sstable_path) = setup();
        let mut memtable: MemTable<String, String> = MemTable::new();
        
        for i in 0..1000 {
            memtable.put(format!("key-{:05}", i), Arc::new(format!("val-{}", i)));
        }
        
        let sst = SSTable::write_from_memtable(&sstable_path, &memtable, SSTableId(1)).unwrap();
        
        assert!(sst.index.len() < 100);
        assert!(sst.index.len() > 1);

        let val = sst.get(&"key-00500".to_string()).unwrap().unwrap();
        assert_eq!(val.value.unwrap().as_ref(), &"val-500".to_string());

        let count = sst.iter().unwrap().count();
        assert_eq!(count, 1000);
    }
}
