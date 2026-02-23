use crate::types::LogEntry;
use crc32fast::Hasher;
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// `Wal` provides a durable, write-ahead log.
pub struct Wal<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    path: PathBuf,
    writer: BufWriter<File>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Wal<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// Creates a brand new, empty WAL file.
    /// If a file already exists at the path, it will be truncated (emptied).
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Opens an existing WAL file for appending. Fails if the file does not exist.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).append(true).open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Appends a single `LogEntry` to the WAL's buffer.
    /// This is not guaranteed to be on disk until `flush()` is called.
    pub fn append(&mut self, entry: &LogEntry<K, V>) -> io::Result<()> {
        // Serialize the LogEntry
        let serialized_entry =
            bincode::serialize(entry).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let entry_len = serialized_entry.len() as u64;

        // Calculate the checksum of the data
        let mut hasher = Hasher::new();
        hasher.update(&serialized_entry);
        let checksum = hasher.finalize();

        // Write the record to the buffer: [Checksum (4), Length (8), Data]
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&entry_len.to_le_bytes())?;
        self.writer.write_all(&serialized_entry)?;

        Ok(())
    }

    pub fn clear(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Flushes all buffered writes to the OS and ensures they are written to disk.
    /// This is the "commit" point for durability.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Returns an iterator that can read all log entries from the beginning of the file.
    /// This is used for database recovery on startup.
    pub fn iter(&self) -> io::Result<WalIterator<K, V>> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
            _phantom: PhantomData,
        })
    }
}

/// An iterator over the entries in a WAL file.
pub struct WalIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    reader: BufReader<File>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Iterator for WalIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = io::Result<LogEntry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read checksum (4 bytes)
        let mut checksum_bytes = [0u8; 4];
        // If we can't read 4 bytes, it means we've reached the end of the file or there's an error.
        // `read_exact` returns `Err` on EOF if fewer than 4 bytes can be read.
        if let Err(e) = self.reader.read_exact(&mut checksum_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None; // Clean EOF
            }
            return Some(Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to read checksum: {}", e),
            )));
        }
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        // Read entry length (8 bytes)
        let mut len_bytes = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut len_bytes) {
            return Some(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof, // Or other error, but EOF is common here
                format!("Failed to read log entry length: {}", e),
            )));
        }
        let entry_len = u64::from_le_bytes(len_bytes) as usize;

        // Read serialized entry data
        let mut serialized_entry = vec![0; entry_len];
        if let Err(e) = self.reader.read_exact(&mut serialized_entry) {
            return Some(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof, // Or other error
                format!("Failed to read log entry data: {}", e),
            )));
        }

        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&serialized_entry);
        if hasher.finalize() != expected_checksum {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL entry checksum mismatch",
            )));
        }

        // Deserialize entry and return it
        match bincode::deserialize(&serialized_entry) {
            Ok(entry) => Some(Ok(entry)),
            Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogEntry;
    use std::{io::Seek, sync::Arc};
    use tempfile::TempDir;

    fn setup() -> (TempDir, PathBuf) {
        let tmp_dir = TempDir::new().expect("Failed to create temporary directory");
        let wal_path = tmp_dir.path().join("wal.log");
        (tmp_dir, wal_path)
    }

    #[test]
    fn create_and_append_flush() {
        let (_tmp_dir, wal_path) = setup();
        let mut wal: Wal<String, String> = Wal::create(&wal_path).expect("Failed to create WAL");

        let key1 = "key1".to_string();
        let val1 = Arc::new("value1".to_string());
        let entry1 = LogEntry::Put(key1.clone(), val1.clone());
        wal.append(&entry1).expect("Failed to write log entry");

        let key2 = "key2".to_string();
        let val2 = Arc::new("value2".to_string());
        let entry2 = LogEntry::Put(key2.clone(), val2.clone());
        wal.append(&entry2).expect("Failed to write log entry");

        wal.flush().expect("Failed to flush WAL");

        let metadata = std::fs::metadata(&wal_path).expect("Failed to get WAL metadata");
        assert!(metadata.len() > 0);
    }

    #[test]
    fn recovery_and_iter() {
        let (_tmp_dir, wal_path) = setup();

        {
            let mut wal: Wal<String, String> =
                Wal::create(&wal_path).expect("Failed to create WAL for writing");

            let entry1 = LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()));
            wal.append(&entry1).expect("Failed to write log entry");
            let entry2 = LogEntry::Delete("k2".to_string());
            wal.append(&entry2).expect("Failed to write log entry");
            let entry3 = LogEntry::Put("k3".to_string(), Arc::new("v3".to_string()));
            wal.append(&entry3).expect("Failed to write log entry");

            wal.flush().expect("Failed to flush WAL");
        } // the wal is closed here

        let wal: Wal<String, String> =
            Wal::open(&wal_path).expect("Failed to create Wal for writing");
        let mut wal_iter = wal.iter().expect("Failed to create WAL iterator");

        let entry1_read = wal_iter
            .next()
            .expect("Expected entry1")
            .expect("Entry1 read failed");
        assert_eq!(
            entry1_read,
            LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()))
        );

        let entry2_read = wal_iter
            .next()
            .expect("Expected entry2")
            .expect("Entry2 read failed");
        assert_eq!(entry2_read, LogEntry::Delete("k2".to_string()));

        let entry3_read = wal_iter
            .next()
            .expect("Expected entry3")
            .expect("Entry3 read failed");
        assert_eq!(
            entry3_read,
            LogEntry::Put("k3".to_string(), Arc::new("v3".to_string()))
        );

        assert!(wal_iter.next().is_none(), "Expected no more entries");
    }

    #[test]
    fn clear() {
        let (_tmp_dir, wal_path) = setup();
        let mut wal: Wal<String, String> = Wal::create(&wal_path).expect("Failed to create WAL");
        let entry1 = LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()));
        let entry2 = LogEntry::Delete("k2".to_string());
        let entry3 = LogEntry::Put("k3".to_string(), Arc::new("v3".to_string()));

        wal.append(&entry1).expect("Failed to write log entry");
        wal.append(&entry2).expect("Failed to write log entry");
        wal.append(&entry3).expect("Failed to write log entry");

        wal.flush().expect("Failed to flush WAL");

        wal.clear().expect("Failed to clear WAL");

        let metadata = std::fs::metadata(&wal_path).expect("Failed to get WAL metadata");
        assert_eq!(metadata.len(), 0);
    }

    #[test]
    fn corrupt_entry() {
        let (_tmp_dir, wal_path) = setup();

        {
            let mut wal: Wal<String, String> =
                Wal::create(&wal_path).expect("Failed to create WAL");

            let entry1 = LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()));
            wal.append(&entry1).expect("Failed to write log entry");
            wal.flush().expect("Failed to flush WAL");
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("Failed to open WAL file for corruption");
        file.seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek to start of WAL file");
        file.write_all(&[0x00, 0x00, 0x00, 0x00])
            .expect("Failed to corrupt checksum"); // Corrupt first 4 bytes

        let wal: Wal<String, String> =
            Wal::open(&wal_path).expect("Failed to open WAL for reading");
        let mut wal_iter = wal.iter().expect("Failed to create WAL iterator");
        let result = wal_iter
            .next()
            .expect("Expected an entry, potentially corrupted");
        assert!(
            result.is_err(),
            "Expected an error due to checksum mismatch"
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
