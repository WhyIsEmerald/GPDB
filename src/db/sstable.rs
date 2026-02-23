use crate::db::memtable::MemTable;
use crate::types::{DBKey, Entry};
use bincode;
use crc32fast::Hasher;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// The fixed size of the footer (24bytes)
///
/// Contains (index_offset: u64, index_size: u64, magic_number: u64)
const FOOTER_SIZE: u64 = 8 + 8 + 8;
/// Unique identifier for sstable files
const MAGIC_NUMBER: u64 = 0xDEADC0DEBEEFCAFE;

pub struct SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    path: PathBuf,
    reader: BufReader<File>,
    index: BTreeMap<K, u64>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> SSTable<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    /// Opens an exisisting SSTable file and loads it into memory.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let file_len = reader.seek(SeekFrom::End(0))?;
        if file_len < FOOTER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SSTable is too small to contain a valid footer",
            ));
        }
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf: [u8; 8] = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let index_offset = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let index_size = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let magic_number = u64::from_le_bytes(buf);
        if magic_number != MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid magic number",
            ));
        }

        reader.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0; index_size as usize];
        reader.read_exact(&mut index_data)?;
        let index: BTreeMap<K, u64> = bincode::deserialize(&index_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        return Ok(SSTable {
            path: path.to_path_buf(),
            reader,
            index,
            _phantom: PhantomData,
        });
    }

    /// Returns the path of this SSTable file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of entries in this SSTable.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Retrieves an Entry for a given key from SSTable file on disk.
    pub fn get(&self, key: &K) -> io::Result<Option<Entry<V>>> {
        let offset = match self.index.get(key) {
            Some(offset) => *offset,
            None => return Ok(None),
        };
        let mut reader = BufReader::new(File::open(&self.path)?);
        reader.seek(SeekFrom::Start(offset))?;

        // Read the checksum
        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        // Read the length of the data
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let entry_len = u64::from_le_bytes(len_bytes) as usize;

        // Read the actual data
        let mut serialized_entry = vec![0; entry_len];
        reader.read_exact(&mut serialized_entry)?;

        // Verify the checksum
        let mut hasher = Hasher::new();
        hasher.update(&serialized_entry);
        if hasher.finalize() != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        let entry: Entry<V> = bincode::deserialize(&serialized_entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(entry))
    }

    /// Writes a new SSTable from the contents of a MemTable.
    /// Returns the opened SSTable instance.
    pub fn write_from_memtable(path: &Path, memtable: &MemTable<K, V>) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let mut index = BTreeMap::new();

        let mut current_offset = 0;
        for (key, entry) in memtable.iter() {
            let serialized_entry = bincode::serialize(entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut hasher = Hasher::new();
            hasher.update(&serialized_entry);
            let checksum = hasher.finalize();
            let len = serialized_entry.len() as u64;

            writer.write_all(&checksum.to_le_bytes())?;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(&serialized_entry)?;

            index.insert(key.clone(), current_offset);
            current_offset += 4 + 8 + len;
        }
        let index_offset = current_offset;
        let serialized_index = bincode::serialize(&index)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let index_size = serialized_index.len() as u64;
        writer.write_all(&serialized_index)?;
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&index_size.to_le_bytes())?;
        writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;
        writer.flush()?;

        Self::open(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::memtable::MemTable;
    use crate::types::DBKey;
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
            SSTable::write_from_memtable(&sstable_path, &memtable)
                .expect("Failed to write to SSTable");

        assert_eq!(sstable.len(), 2);

        let sstable: SSTable<String, String> =
            SSTable::open(&sstable_path).expect("Failed to open SSTable");
        assert_eq!(sstable.len(), 2);
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
            SSTable::write_from_memtable(&sstable_path, &memtable)
                .expect("Failed to write to SSTable");

        assert_eq!(sstable.len(), 2);

        let sstable: SSTable<String, String> =
            SSTable::open(&sstable_path).expect("Failed to open SSTable");

        let entry1 = sstable
            .get(&"key1".to_string())
            .expect("Failed to get k1")
            .expect("k1 not found");
        assert_eq!(entry1.value.unwrap().as_ref(), &"value1".to_string());
        assert!(!entry1.is_tombstone);

        let entry2 = sstable
            .get(&"key2".to_string())
            .expect("Failed to get k2")
            .expect("k2 not found");
        assert!(entry2.is_tombstone);
        assert_eq!(entry2.value, None);

        let entry3 = sstable.get(&"key3".to_string()).expect("Failed to get k3");
        assert!(entry3.is_none());
    }
}
