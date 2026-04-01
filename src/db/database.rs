use crate::db::compaction::Compactor;
use crate::db::inner::{DBInner, MANIFEST_FILE_NAME, WAL_FILE_NAME};
use crate::{DBKey, LogEntry, Manifest, ManifestEntry, MemTable, Result, SSTable, SSTableId, Wal};
use parking_lot::RwLock;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

/// The public handle to the database.
///
/// This struct is thread-safe and can be cheaply cloned to share the
/// database instance across multiple threads. It uses an internal
/// RwLock to allow multiple concurrent readers.
#[derive(Debug, Clone)]
pub struct DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    inner: Arc<RwLock<DBInner<K, V>>>,
}

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    /// Opens the database at a given path.
    ///
    /// # Arguments
    /// * `path` - The directory where the database files are stored.
    /// * `max_memtable_size` - The threshold in bytes before flushing the MemTable to disk.
    pub fn open(path: &Path, max_memtable_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;

        let manifest_path = path.join(MANIFEST_FILE_NAME);
        let manifest = if manifest_path.exists() {
            Manifest::open(manifest_path)?
        } else {
            Manifest::create(manifest_path)?
        };

        let mut levels: Vec<Vec<SSTable<K, V>>> = vec![Vec::new()];
        let mut next_id = SSTableId(0);
        let mut active_sstables: HashSet<(usize, PathBuf)> = HashSet::new();

        for record_result in manifest.iter()? {
            let record =
                record_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            match record {
                ManifestEntry::AddSSTable { level, path } => {
                    active_sstables.insert((level, path.clone()));
                }
                ManifestEntry::RemoveSSTable { level, path } => {
                    active_sstables.remove(&(level, path.clone()));
                }
                ManifestEntry::NextID(id) => {
                    next_id = id;
                }
            }
        }

        for (level, rel_path) in active_sstables {
            if level >= levels.len() {
                levels.resize_with(level + 1, Vec::new);
            }
            let full_path = path.join(rel_path);
            let sstable = SSTable::open(&full_path)?;
            levels[level].push(sstable);
        }

        for level_sstables in &mut levels {
            level_sstables.sort_by_key(|sst| sst.id());
        }

        let wal_path = path.join(WAL_FILE_NAME);
        let mut memtable = MemTable::new();
        let wal: Wal<K, V>;

        if wal_path.exists() {
            let existing_wal = Wal::open(&wal_path)?;
            for entry_result in existing_wal.iter()? {
                let entry =
                    entry_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                match entry {
                    LogEntry::Put(k, v) => memtable.put(k, v),
                    LogEntry::Delete(k) => memtable.delete(k),
                };
            }
            wal = Wal::open(&wal_path)?;
        } else {
            wal = Wal::create(&wal_path)?;
        }

        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        std::thread::spawn(move || {
            Compactor::run_worker::<K, V>(task_rx, result_tx);
        });

        let inner = DBInner {
            path: path.to_path_buf(),
            memtable,
            wal,
            manifest: parking_lot::Mutex::new(manifest),
            levels,
            next_id,
            max_memtable_size,
            memtable_size: 0,
            compaction_tx: task_tx,
            compaction_rx: parking_lot::Mutex::new(result_rx),
            compacting_ids: HashSet::new(),
        };

        Ok(DB {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    /// Puts a key-value pair into the database.
    ///
    /// # Arguments
    /// * `key` - The key to insert.
    /// * `value` - The value to associate with the key.
    pub fn put(&self, key: K, value: V) -> Result<()> {
        let mut inner = self.inner.write();
        inner.put(key, value)
    }

    /// Deletes a key from the database.
    ///
    /// # Arguments
    /// * `key` - The key to remove.
    pub fn delete(&self, key: K) -> Result<()> {
        let mut inner = self.inner.write();
        inner.delete(key)
    }

    /// Retrieves a value for a given key from the database.
    ///
    /// # Arguments
    /// * `key` - The key to look up.
    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>> {
        let inner = self.inner.read();
        inner.get(key)
    }

    /// Checks for finished background compactions and applies them.
    ///
    /// This method is called internally during writes, but can be called
    /// manually to ensure background work is processed during idle periods.
    pub fn handle_compaction_results(&self) -> Result<()> {
        let mut inner = self.inner.write();
        inner.handle_compaction_results()
    }

    /// Returns the number of SSTables currently in the compaction pipeline.
    pub fn compaction_backlog(&self) -> usize {
        let inner = self.inner.read();
        inner.compaction_backlog()
    }

    /// Returns the total number of SSTables across all levels.
    pub fn total_sst_count(&self) -> usize {
        let inner = self.inner.read();
        inner.total_sst_count()
    }
}
