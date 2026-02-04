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
use std::sync::Arc;

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
                "SSTable is too small / lacks footer",
            ));
        }
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf: [u8; 8] = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let mut index_offset = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let mut index_size = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let mut magic_number = u64::from_le_bytes(buf);
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

    /// Retrieves an Entry for a given key from SSTable file on disk.
    pub fn get(&mut self, key: &K) -> io::Result<Option<Entry<V>>> {
        let offset = match self.index.get(key) {
            Some(offset) => *offset,
            None => return Ok(None),
        };
        self.reader.seek(SeekFrom::Start(offset))?;

        // Read the checksums
        let mut checksum_bytes = [0u8; 4];
        self.reader.read_exact(&mut checksum_bytes)?;
        let checksum = u32::from_le_bytes(checksum_bytes);

        // Read the length of the data
        let mut len = [0u8; 8];
        self.reader.read_exact(&mut len)?;
        let len = u64::from_le_bytes(len) as usize;

        // Read the actual data
        let mut serialized_entry = vec![0; len];
        self.reader.read_exact(&mut serialized_entry)?;

        // Verify the checksums
        let mut hasher = Hasher::new();
        hasher.update(&serialized_entry);
        if hasher.finalize() != checksum {
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

            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(&checksum.to_le_bytes())?;
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
