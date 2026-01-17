use crate::db::memtable::MemTable;
use crate::types::{DBKey, Entry};
use std::io;
use std::path::{Path, PathBuf};

pub struct DB<K, V>
where
    K: DBKey,
    V: Clone,
{
    path: PathBuf,
    memtable: MemTable<K, V>,
}

impl<K, V> DB<K, V>
where
    K: DBKey,
    V: Clone,
{
    pub fn open(path: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
        Ok(DB {
            path: path.to_path_buf(),
            memtable: MemTable::new(),
        })
    }

    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        self.memtable.put(key, value);
        Ok(())
    }

    pub fn get(&self, key: &K) -> io::Result<Option<V>> {
        Ok(self.memtable.get(key).cloned())
    }
}
