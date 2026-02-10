use crate::db::memtable::MemTable;
use crate::db::sstable::SSTable;
use crate::db::wal::Wal;
use crate::types::{DBKey, LogEntry};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The main database struct, which orchestrates the MemTable, WAL, and SSTables.
pub struct DB<K, V>
where
    K: DBKey,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    path: PathBuf,
    memtable: MemTable<K, V>,
    wal: Wal<K, V>,
    sstables: Vec<SSTable<K, V>>,
    max_memtable_size: usize,
    memtable_size: usize,
}

impl<K, V> DB<K, V>
where
    K: DBKey,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Opens the database at a given path.
    /// This will recover from the WAL if it exists.
    pub fn open(path: &Path, max_memtable_size: usize) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;

        let mut sstables = Vec::new();

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_file() && entry_path.extension().unwrap_or_default() == "sst" {
                println!("Loading SSTable: {:?}", entry_path);
                let sstable = SSTable::open(&path)?;
                sstables.push(sstable);
            }
        }

        sstables.sort_by(|a, b| a.path().cmp(&b.path()));

        let wal_path = path.join("wal.log");
        let mut memtable = MemTable::new();
        let wal: Wal<K, V>;

        if wal_path.exists() {
            println!("Recovering from WAL: {:?}", wal_path);
            let existing_wal = Wal::open(&wal_path)?;
            for entry_result in existing_wal.iter()? {
                let entry = entry_result?;
                match entry {
                    LogEntry::Put(k, v) => {
                        memtable.put(k, v);
                    }
                    LogEntry::Delete(k) => {
                        memtable.delete(k);
                    }
                }
            }
            // After successful recovery, re-open the WAL in append mode.
            wal = Wal::open(&wal_path)?;
        } else {
            println!("Creating new WAL: {:?}", wal_path);
            wal = Wal::create(&wal_path)?;
        }

        Ok(DB {
            path: path.to_path_buf(),
            memtable,
            wal,
            sstables,
            max_memtable_size,
            memtable_size: 0,
        })
    }

    /// Puts a key-value pair into the database.
    /// The operation is first written to the WAL for durability, then applied to the MemTable.
    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let value_size = std::mem::size_of_val(&value);
        let key_size = std::mem::size_of_val(&key);
        let arc_value = Arc::new(value);
        let log_entry = LogEntry::Put(key.clone(), arc_value.clone());

        self.memtable_size += key_size + value_size;
        self.wal.append(&log_entry)?;
        self.wal.flush()?;
        self.memtable.put(key, arc_value);
        if self.memtable_size > self.max_memtable_size {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Deletes a key from the database.
    /// A tombstone is written to the WAL and MemTable.
    pub fn delete(&mut self, key: K) -> io::Result<()> {
        let log_entry = LogEntry::Delete(key.clone());
        self.wal.append(&log_entry)?;
        self.wal.flush()?;
        self.memtable.delete(key);
        Ok(())
    }

    /// Retrieves a value for a given key from the database.
    pub fn get(&self, key: &K) -> io::Result<Option<Arc<V>>> {
        if let Some(value_arc) = self.memtable.get(key) {
            return Ok(Some(value_arc));
        }

        // Check the SSTables, from newest to oldest
        for sstable in self.sstables.iter().rev() {
            if let Some(entry) = sstable.get(key)? {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(entry.value);
            }
        }

        Ok(None)
    }

    /// Flushes the MemTable to an SSTable.
    fn flush_memtable(&mut self) -> io::Result<()> {
        println!("MemTable has reached size limit, flushing to SSTable...");
        let sstable_path = self.path.join(format!(
            "{:020}.sst",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let new_sstable = SSTable::write_from_memtable(&sstable_path, &self.memtable)?;
        self.sstables.push(new_sstable);

        self.memtable.clear();
        self.memtable_size = 0;

        self.wal.clear()?;

        Ok(())
    }
}
