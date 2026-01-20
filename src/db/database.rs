use crate::db::memtable::MemTable;
use crate::db::wal::Wal;
use crate::types::{DBKey, Entry, LogEntry};

use std::io;
use std::path::{Path, PathBuf};

pub struct DB<K, V>
where
    K: DBKey,
    V: Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    path: PathBuf,
    memtable: MemTable<K, V>,
    wal: Wal<K, V>,
}

impl<K, V> DB<K, V>
where
    K: DBKey,
    V: Clone + serde::Serialize + serde::de::DeserializeOwned,
{
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
                    LogEntry::Put(k, v) => memtable.put(k, v),
                    LogEntry::Delete(k) => memtable.delete(k),
                };
            }
            wal = Wal::create(&wal_path)?;
        } else {
            println!("Creating new WAL: {:?}", wal_path);
            wal = Wal::create(&wal_path)?;
        }

        Ok(DB {
            path: path.to_path_buf(),
            memtable: memtable,
            wal,
        })
    }

    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let log_entry = LogEntry::Put(key.clone(), value.clone());

        self.wal.append(&log_entry)?;

        self.wal.flush()?;

        self.memtable.put(key, value);

        Ok(())
    }

    pub fn delete(&mut self, key: K) -> io::Result<()> {
        let log_entry = LogEntry::Delete(key.clone());
        self.wal.append(&log_entry)?;
        self.wal.flush()?;
        self.memtable.delete(key);
        Ok(())
    }

    pub fn get(&self, key: &K) -> io::Result<Option<V>> {
        Ok(self.memtable.get(key).cloned())
    }
}
