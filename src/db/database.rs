use crate::db::memtable::MemTable;
use crate::db::wal::Wal;
use crate::types::{DBKey, Entry, LogEntry};
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
}

impl<K, V> DB<K, V>
where
    K: DBKey,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Opens the database at a given path.
    /// This will recover from the WAL if it exists.
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
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
        })
    }

    /// Puts a key-value pair into the database.
    /// The operation is first written to the WAL for durability, then applied to the MemTable.
    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let arc_value = Arc::new(value);
        let log_entry = LogEntry::Put(key.clone(), arc_value.clone());
        self.wal.append(&log_entry)?;
        self.wal.flush()?;
        self.memtable.put(key, arc_value);
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
    /// Currently only checks the MemTable.
    pub fn get(&self, key: &K) -> io::Result<Option<Arc<V>>> {
        Ok(self.memtable.get(key))
    }
}
