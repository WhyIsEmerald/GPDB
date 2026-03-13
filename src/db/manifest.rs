use crate::{ManifestEntry, Result};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Manifest {
    /// Creates a brand new, empty Manifest file.
    /// If a file already exists at the path, it will be truncated (emptied).
    pub fn create(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let writer = BufWriter::new(file);

        Ok(Manifest { path, writer })
    }

    /// Opens an existing Manifest file for appending. Fails if the file does not exist.
    pub fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().write(true).append(true).open(&path)?;

        let writer = BufWriter::new(file);

        Ok(Manifest { path, writer })
    }

    /// Appends a single `ManifestEntry` to the Manifest's buffer.
    /// This is not guaranteed to be on disk until `flush()` is called.
    pub fn append(&mut self, entry: &ManifestEntry) -> Result<()> {
        // Serialize the ManifestEntry
        let serialized_entry =
            bincode::serialize(entry).map_err(|e| crate::Error::Serialization(e.to_string()))?;
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

    /// Clears the Manifest's buffer.
    pub fn clear(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Flushes the Manifest's buffer to disk and ensures it's physically synced.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?; // Flush BufWriter's internal buffer
        self.writer.get_ref().sync_all()?; // Ensure data is on physical disk
        Ok(())
    }

    /// Returns an iterator that can read all manifest entries from the beginning of the file.
    /// This is used for database state recovery on startup.
    pub fn iter(&self) -> Result<ManifestIterator> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(ManifestIterator {
            reader: BufReader::new(file),
        })
    }
}

/// An iterator over the entries in a Manifest file.
pub struct ManifestIterator {
    reader: BufReader<File>,
}

impl Iterator for ManifestIterator {
    type Item = std::result::Result<ManifestEntry, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read checksum (4 bytes)
        let mut checksum_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut checksum_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None; // Clean EOF
            }
            return Some(Err(crate::Error::Io(e)));
        }
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        // Read record length (8 bytes)
        let mut len_bytes = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut len_bytes) {
            return Some(Err(crate::Error::Io(e)));
        }
        let record_len = u64::from_le_bytes(len_bytes) as usize;

        // Read serialized record data
        let mut serialized_entry = vec![0; record_len];
        if let Err(e) = self.reader.read_exact(&mut serialized_entry) {
            return Some(Err(crate::Error::Io(e)));
        }

        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&serialized_entry);
        if hasher.finalize() != expected_checksum {
            return Some(Err(crate::Error::Corruption(
                "Manifest record checksum mismatch".to_string(),
            )));
        }

        // Deserialize entry and return it
        match bincode::deserialize(&serialized_entry) {
            Ok(entry) => Some(Ok(entry)),
            Err(e) => Some(Err(crate::Error::Serialization(e.to_string()))),
        }
    }
}
