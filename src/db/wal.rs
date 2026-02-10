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
