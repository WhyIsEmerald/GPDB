use crate::{Manifest, MemTable, SSTable, Wal, DBKey, LogEntry, ManifestEntry, SSTableId, Result};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MANIFEST_FILE_NAME: &str = "MANIFEST";
const WAL_FILE_NAME: &str = "wal.log";

/// The main database struct, which orchestrates the MemTable, WAL, and SSTables.
pub struct DB<K, V>
where
    K: DBKey,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    path: PathBuf,
    memtable: MemTable<K, V>,
    wal: Wal<K, V>,
    manifest: Manifest,
    levels: Vec<Vec<SSTable<K, V>>>,
    next_id: SSTableId,
    max_memtable_size: usize,
    memtable_size: usize,
}

impl<K, V> DB<K, V>
where
    K: DBKey,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Opens the database at a given path.
    /// This will recover state from the Manifest and data from the WAL.
    /// 
    /// # Example
    /// ```
    /// # use gpdb::DB;
    /// # use tempfile::TempDir;
    /// # let tmp_dir = TempDir::new().unwrap();
    /// # let path = tmp_dir.path();
    /// let db: DB<String, String> = DB::open(path, 1024 * 1024).expect("Failed to open DB");
    /// ```
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

        // Replay manifest to recover state
        for record_result in manifest.iter()? {
            let record = record_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            match record {
                ManifestEntry::AddSSTable { level, path } => {
                    if level >= levels.len() {
                        levels.resize_with(level + 1, Vec::new);
                    }
                    let sstable = SSTable::open(&path)?;
                    levels[level].push(sstable);
                }
                ManifestEntry::RemoveSSTable { level, path } => {
                    if let Some(level_sstables) = levels.get_mut(level) {
                        level_sstables.retain(|sst| sst.path() != path);
                    }
                }
                ManifestEntry::NextID(id) => {
                    next_id = id;
                }
            }
        }

        // Sort SSTables within each level by ID (which is sequential)
        for level_sstables in &mut levels {
            level_sstables.sort_by_key(|sst| sst.id());
        }

        let wal_path = path.join(WAL_FILE_NAME);
        let mut memtable = MemTable::new();
        let wal: Wal<K, V>;

        if wal_path.exists() {
            let existing_wal = Wal::open(&wal_path)?;
            for entry_result in existing_wal.iter()? {
                let entry = entry_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                match entry {
                    LogEntry::Put(k, v) => {
                        memtable.put(k, v);
                    }
                    LogEntry::Delete(k) => {
                        memtable.delete(k);
                    }
                }
            }
            wal = Wal::open(&wal_path)?;
        } else {
            wal = Wal::create(&wal_path)?;
        }

        Ok(DB {
            path: path.to_path_buf(),
            memtable,
            wal,
            manifest,
            levels,
            next_id,
            max_memtable_size,
            memtable_size: 0,
        })
    }

    /// Puts a key-value pair into the database.
    ///
    /// # Example
    /// ```
    /// # use gpdb::DB;
    /// # use tempfile::TempDir;
    /// # let tmp_dir = TempDir::new().unwrap();
    /// # let path = tmp_dir.path();
    /// let mut db = DB::open(path, 1024 * 1024).unwrap();
    /// db.put("key".to_string(), "value".to_string()).unwrap();
    /// ```
    pub fn put(&mut self, key: K, value: V) -> Result<()> {
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
    pub fn delete(&mut self, key: K) -> Result<()> {
        let log_entry = LogEntry::Delete(key.clone());
        self.wal.append(&log_entry)?;
        self.wal.flush()?;
        self.memtable.delete(key);
        Ok(())
    }

    /// Retrieves a value for a given key from the database.
    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>> {
        if let Some(value_arc) = self.memtable.get(key) {
            return Ok(Some(value_arc));
        }

        // Check the SSTables, from newest to oldest
        for level_sstables in self.levels.iter() {
            for sstable in level_sstables.iter().rev() {
                if let Some(value_entry) = sstable.get(key)? {
                    if value_entry.is_tombstone {
                        return Ok(None);
                    }
                    return Ok(value_entry.value);
                }
            }
        }

        Ok(None)
    }

    /// Flushes the MemTable to an SSTable and updates the Manifest.
    fn flush_memtable(&mut self) -> Result<()> {
        let id = self.next_id;
        let sstable_path = self.path.join(format!("L0-{}.sst", id));

        // 1. Write SSTable
        let new_sstable = SSTable::write_from_memtable(&sstable_path, &self.memtable, id)?;

        // 2. Update Manifest BEFORE updating in-memory state
        self.manifest.append(&ManifestEntry::AddSSTable {
            level: 0,
            path: sstable_path.clone(),
        })?;
        self.next_id = SSTableId(id.0 + 1);
        self.manifest.append(&ManifestEntry::NextID(self.next_id))?;
        self.manifest.flush()?;

        // 3. Update in-memory SSTable list
        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].push(new_sstable);
        self.levels[0].sort_by_key(|sst| sst.id());

        // 4. Clear MemTable and WAL
        self.memtable.clear();
        self.memtable_size = 0;
        self.wal.clear()?;

        Ok(())
    }
}
