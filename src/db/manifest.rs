//! Manifest implementation.
//!
//! Provides a durable log of database state changes, such as SSTable additions and removals.

use crate::db::io::{read_record, write_record};
use crate::{ManifestEntry, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

/// A struct that represents a durable log that tracks the current state of the database's SSTables.
#[derive(Debug)]
pub struct Manifest {
    /// The path used for the manifest file on disk.
    path: PathBuf,
    /// The writer used for appending manifest entries.
    writer: BufWriter<File>,
}

impl Manifest {
    /// Creates a new manifest file at the specified path.
    pub fn create(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(Manifest {
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Opens an existing manifest file for appending.
    pub fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().append(true).open(&path)?;
        Ok(Manifest {
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Appends a new entry to the manifest.
    pub fn append(&mut self, entry: &ManifestEntry) -> Result<()> {
        write_record(&mut self.writer, entry)?;
        Ok(())
    }

    /// Clears the manifest file.
    pub fn clear(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Flushes the manifest buffer to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Returns an iterator over all entries in the manifest.
    pub fn iter(&self) -> Result<ManifestIterator> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(ManifestIterator {
            reader: BufReader::new(file),
        })
    }
}

/// A struct that represents an iterator over the entries of a manifest file.
pub struct ManifestIterator {
    /// The reader used for the manifest file.
    reader: BufReader<File>,
}

impl Iterator for ManifestIterator {
    type Item = std::result::Result<ManifestEntry, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        read_record(&mut self.reader).transpose()
    }
}
