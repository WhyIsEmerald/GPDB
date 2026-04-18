use crate::db::io::{read_record, write_record};
use crate::{Error, LogEntry, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// `Wal` provides a durable, write-ahead log.
#[derive(Debug)]
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
    pub fn create(path: &Path) -> Result<Self> {
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

    /// Opens an existing WAL file for appending.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().write(true).append(true).open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Appends a single `LogEntry` to the WAL's buffer.
    pub fn append(&mut self, entry: &LogEntry<K, V>) -> Result<()> {
        write_record(&mut self.writer, entry)?;
        Ok(())
    }

    /// Appends multiple entries to the WAL in a single buffered operation.
    pub fn append_batch(&mut self, entries: &[LogEntry<K, V>]) -> Result<()> {
        for entry in entries {
            write_record(&mut self.writer, entry)?;
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Flushes all buffered writes to the OS and ensures they are written to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Returns an iterator that can read all log entries from the beginning of the file.
    pub fn iter(&self) -> Result<WalIterator<K, V>> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
            _phantom: PhantomData,
        })
    }
}

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
    type Item = std::result::Result<LogEntry<K, V>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        read_record(&mut self.reader).transpose()
    }
}
