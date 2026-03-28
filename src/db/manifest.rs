use crate::db::io::{read_record, write_record};
use crate::{ManifestEntry, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug)]
pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Manifest {
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

    pub fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().write(true).append(true).open(&path)?;
        Ok(Manifest {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, entry: &ManifestEntry) -> Result<()> {
        write_record(&mut self.writer, entry)?;
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

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn iter(&self) -> Result<ManifestIterator> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(ManifestIterator {
            reader: BufReader::new(file),
        })
    }
}

pub struct ManifestIterator {
    reader: BufReader<File>,
}

impl Iterator for ManifestIterator {
    type Item = std::result::Result<ManifestEntry, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        read_record(&mut self.reader).transpose()
    }
}
