use crate::db::compaction::Compactor;
use crate::{DBKey, LogEntry, Manifest, ManifestEntry, MemTable, Result, SSTable, SSTableId, Wal};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MANIFEST_FILE_NAME: &str = "MANIFEST";
const WAL_FILE_NAME: &str = "wal.log";
const L0_COMPACTION_THRESHOLD: usize = 4;

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

        for entry in active_sstables.iter() {
            match entry {
                (level, rel_path) => {
                    if level >= &levels.len() {
                        levels.resize_with(*level + 1, Vec::new);
                    }
                    let full_path = path.join(rel_path);
                    let sstable = SSTable::open(&full_path)?;
                    levels[*level].push(sstable);
                }
            }
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
            if self.levels[0].len() >= L0_COMPACTION_THRESHOLD {
                self.run_compaction()?;
            }
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
        if let Some(entry) = self.memtable.get_entry(key) {
            if entry.is_tombstone {
                return Ok(None);
            }
            return Ok(entry.value.clone());
        }

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
        let filename = format!("L0-{}.sst", id);
        let sstable_path = self.path.join(&filename);

        let new_sstable = SSTable::write_from_memtable(&sstable_path, &self.memtable, id)?;

        self.manifest.append(&ManifestEntry::AddSSTable {
            level: 0,
            path: PathBuf::from(filename),
        })?;
        self.next_id = SSTableId(id.0 + 1);
        self.manifest.append(&ManifestEntry::NextID(self.next_id))?;
        self.manifest.flush()?;

        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].push(new_sstable);
        self.levels[0].sort_by_key(|sst| sst.id());

        self.memtable.clear();
        self.memtable_size = 0;
        self.wal.clear()?;

        Ok(())
    }

    /// Merges all L0 SSTables into a single L1 SSTable.
    fn run_compaction(&mut self) -> Result<()> {
        let l0_sstables = &self.levels[0];
        let id = self.next_id;
        let filename = format!("L1-{}.sst", id);
        let l1_path = self.path.join(&filename);

        let new_l1_sstable = Compactor::compact_l0(l0_sstables, &l1_path, id)?;

        // Update Manifest
        for sst in l0_sstables {
            if let Some(file_name) = sst.path().file_name() {
                self.manifest.append(&ManifestEntry::RemoveSSTable {
                    level: 0,
                    path: PathBuf::from(file_name),
                })?;
            }
        }
        self.manifest.append(&ManifestEntry::AddSSTable {
            level: 1,
            path: PathBuf::from(filename),
        })?;

        self.next_id = SSTableId(id.0 + 1);
        self.manifest.append(&ManifestEntry::NextID(self.next_id))?;
        self.manifest.flush()?;

        // Update memory state
        let old_l0_paths: Vec<PathBuf> = self.levels[0]
            .iter()
            .map(|s| s.path().to_path_buf())
            .collect();
        self.levels[0].clear();

        if self.levels.len() < 2 {
            self.levels.resize_with(2, Vec::new);
        }
        self.levels[1].push(new_l1_sstable);
        self.levels[1].sort_by_key(|sst| sst.id());

        // Cleanup
        for path in old_l0_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }
}
