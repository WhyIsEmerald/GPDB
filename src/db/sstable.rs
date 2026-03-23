use crate::db::io::{read_record, write_record};
use crate::{DBKey, Entry, Error, MemTable, Result, SSTableId, TableMeta, ValueEntry};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The fixed size of the footer (32 bytes)
///
/// Contains (index_offset: u64, meta_offset: u64, id: u64, magic_number: u64)
const FOOTER_SIZE: u64 = 8 + 8 + 8 + 8;
/// Unique identifier for sstable files
const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFF;

pub struct SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    path: PathBuf,
    // Mutex allows interior mutability so we can seek/read while having a &self reference
    reader: Mutex<BufReader<File>>,
    index: BTreeMap<K, u64>,
    meta: TableMeta<K>,
    id: SSTableId,
    index_offset: u64,
    file_size: u64,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let file_len = reader.seek(SeekFrom::End(0))?;
        if file_len < FOOTER_SIZE {
            return Err(Error::Corruption(
                "SSTable is too small to contain a valid footer".to_string(),
            ));
        }
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf: [u8; 8] = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let index_offset = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let meta_offset = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let id = SSTableId(u64::from_le_bytes(buf));
        reader.read_exact(&mut buf)?;
        let magic_number = u64::from_le_bytes(buf);

        if magic_number != MAGIC_NUMBER {
            return Err(Error::Corruption("Invalid magic number".to_string()));
        }

        // Seek to the index block and read it
        reader.seek(SeekFrom::Start(index_offset))?;
        let index: BTreeMap<K, u64> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable index block is missing or empty".to_string())
        })?;

        // Seek to the meta block and read it
        reader.seek(SeekFrom::Start(meta_offset))?;
        let meta: TableMeta<K> = read_record(&mut reader)?.ok_or_else(|| {
            Error::Corruption("SSTable meta block is missing or empty".to_string())
        })?;

        Ok(SSTable {
            path: path.to_path_buf(),
            reader: Mutex::new(reader),
            index,
            meta,
            id,
            index_offset,
            file_size: file_len,
            _phantom: PhantomData,
        })
    }

    /// Returns the path of this SSTable file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the ID of this SSTable.
    pub fn id(&self) -> SSTableId {
        self.id
    }

    /// Returns the number of entries in this SSTable.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns the file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the minimum key in this SSTable.
    pub fn min_key(&self) -> &K {
        &self.meta.min_key
    }

    /// Returns the maximum key in this SSTable.
    pub fn max_key(&self) -> &K {
        &self.meta.max_key
    }

    /// Returns the total number of entries from metadata.
    pub fn num_entries(&self) -> u64 {
        self.meta.num_entries
    }

    /// Checks if this SSTable's key range overlaps with another SSTable.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.overlaps_range(other.min_key(), other.max_key())
    }

    /// Checks if this SSTable's key range overlaps with the given range.
    pub fn overlaps_range(&self, min: &K, max: &K) -> bool {
        // [self.min, self.max] and [min, max] overlap if:
        // self.min <= max AND min <= self.max
        self.min_key() <= max && min <= self.max_key()
    }

    /// Retrieves a ValueEntry for a given key from SSTable file on disk.
    pub fn get(&self, key: &K) -> Result<Option<ValueEntry<V>>> {
        let offset = match self.index.get(key) {
            Some(offset) => *offset,
            None => return Ok(None),
        };

        // Lock the reader and seek to the entry offset
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| Error::Corruption("Lock poisoned".to_string()))?;
        reader.seek(SeekFrom::Start(offset))?;

        let entry: Option<Entry<K, V>> = read_record(&mut *reader)?;
        Ok(entry.map(|e| e.value))
    }

    pub fn write_from_iter<I>(path: &Path, iter: I, id: SSTableId) -> Result<Self>
    where
        K: DBKey,
        V: Serialize + DeserializeOwned,
        I: Iterator<Item = Result<Entry<K, V>>>,
    {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let mut index = BTreeMap::new();

        let mut min_key = None;
        let mut max_key = None;
        let mut num_entries = 0;

        let mut current_offset = 0;
        for item in iter {
            let entry = item?;
            if min_key.is_none() {
                min_key = Some(entry.key.clone());
            }
            max_key = Some(entry.key.clone());
            num_entries += 1;

            let bytes_written = write_record(&mut writer, &entry)?;
            index.insert(entry.key.clone(), current_offset);
            current_offset += bytes_written;
        }

        let min_key = min_key.ok_or_else(|| {
            Error::Corruption("Cannot write an empty SSTable".to_string())
        })?;
        let max_key = max_key.unwrap();

        // Write the index block
        let index_offset = current_offset;
        let index_size = write_record(&mut writer, &index)?;

        // Write the meta block
        let meta_offset = index_offset + index_size;
        let meta = TableMeta {
            min_key,
            max_key,
            num_entries,
        };
        write_record(&mut writer, &meta)?;

        // Write the footer
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&meta_offset.to_le_bytes())?;
        writer.write_all(&id.0.to_le_bytes())?;
        writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;
        writer.flush()?;

        Self::open(path)
    }

    /// Writes a new SSTable from the contents of a MemTable.
    /// Returns the opened SSTable instance.
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

    /// Returns an iterator over the full entries in the SSTable.
    pub fn iter(&self) -> Result<SSTableIterator<K, V>> {
        let file = File::open(&self.path)?;
        Ok(SSTableIterator {
            reader: BufReader::new(file),
            end_offset: self.index_offset,
            _phantom: PhantomData,
        })
    }
}

pub struct SSTableIterator<K, V> {
    reader: BufReader<File>,
    end_offset: u64,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Iterator for SSTableIterator<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Stop if we've reached the start of the index block
        let current_pos = self.reader.stream_position().ok()?;
        if current_pos >= self.end_offset {
            return None;
        }

        read_record(&mut self.reader).transpose()
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
        assert_eq!(sstable.len(), 2);
        assert_eq!(sstable.id(), SSTableId(1));
        assert!(sstable.index.contains_key("key1"));
        assert!(sstable.index.contains_key("key2"));
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
}
