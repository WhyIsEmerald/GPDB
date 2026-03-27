use crate::db::compaction::Compactor;
use crate::{
    CompactionResult, CompactionTask, DBKey, Error, LogEntry, Manifest, ManifestEntry, MemTable,
    Result, SSTable, SSTableId, Wal,
};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};

const MANIFEST_FILE_NAME: &str = "MANIFEST";
const WAL_FILE_NAME: &str = "wal.log";
const L0_COMPACTION_THRESHOLD: usize = 4;
const BASE_LEVEL_SIZE: u64 = 10 * 1024 * 1024; // 10MB
const LEVEL_MULTIPLIER: u32 = 10;

/// The main database struct, which orchestrates the MemTable, WAL, and SSTables.
pub struct DB<K, V>
where
    K: DBKey + Send + 'static,
    V: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    path: PathBuf,
    memtable: MemTable<K, V>,
    wal: Wal<K, V>,
    manifest: Manifest,
    levels: Vec<Vec<SSTable<K, V>>>,
    next_id: SSTableId,
    max_memtable_size: usize,
    memtable_size: usize,
    compaction_tx: Sender<CompactionTask<K, V>>,
    compaction_rx: Receiver<CompactionResult<K, V>>,
}

impl<K, V> DB<K, V>
where
    K: DBKey + Send + 'static,
    V: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    /// Opens the database at a given path.
    ///
    /// This will recover state from the Manifest and data from the WAL.
    /// It also starts the background compaction worker thread.
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

        // Replay Manifest to find which files are currently "live"
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

        // Open all active SSTables
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

        // Keep each level sorted for binary search/iteration
        for level_sstables in &mut levels {
            level_sstables.sort_by_key(|sst| sst.id());
        }

        // Recover un-flushed data from the Write-Ahead Log
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

        // Setup Background Compaction
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        std::thread::spawn(move || {
            Compactor::run_worker::<K, V>(task_rx, result_tx);
        });

        Ok(DB {
            path: path.to_path_buf(),
            memtable,
            wal,
            manifest,
            levels,
            next_id,
            max_memtable_size,
            memtable_size: 0,
            compaction_tx: task_tx,
            compaction_rx: result_rx,
        })
    }

    /// Puts a key-value pair into the database.
    ///
    /// This operation is durable (logged to WAL) and will trigger a flush/compaction if needed.
    ///
    /// # Arguments
    /// * `key` - The key to store.
    /// * `value` - The value associated with the key.
    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        self.handle_compaction_results()?;

        let value_size = std::mem::size_of_val(&value);
        let key_size = std::mem::size_of_val(&key);
        let arc_value = Arc::new(value);
        let log_entry = LogEntry::Put(key.clone(), arc_value.clone());

        // 1. Write to log
        self.memtable_size += key_size + value_size;
        self.wal.append(&log_entry)?;
        self.wal.flush()?;

        // 2. Update memory
        self.memtable.put(key, arc_value);

        // 3. Maintenance check
        if self.memtable_size > self.max_memtable_size {
            self.flush_memtable()?;
            self.check_compaction()?;
        }
        Ok(())
    }

    /// Checks if any level has exceeded its size or count threshold and triggers background work.
    fn check_compaction(&mut self) -> Result<()> {
        // L0 check: Based on file count
        if self.levels[0].len() >= L0_COMPACTION_THRESHOLD {
            let sstables = self.levels[0].drain(..).collect();
            self.trigger_compaction(sstables, 1)?;
        }

        // L1+ check: Based on total byte size
        for level in 1..self.levels.len() {
            if self.level_current_size(level) > self.level_max_size(level) {
                // Pick the first file and its overlaps in the next level
                let candidate = self.levels[level].remove(0);
                let mut sstables = vec![candidate];

                if level + 1 < self.levels.len() {
                    let overlaps =
                        Compactor::find_overlapping_sstables(&sstables[0], &self.levels[level + 1]);
                    let mut sorted_idx = overlaps;
                    sorted_idx.sort_unstable_by(|a, b| b.cmp(a));
                    for idx in sorted_idx {
                        sstables.push(self.levels[level + 1].remove(idx));
                    }
                }
                self.trigger_compaction(sstables, level + 1)?;
            }
        }
        Ok(())
    }

    /// Sends a list of SSTables to the background thread to be merged.
    ///
    /// # Arguments
    /// * `sstables` - The files to merge.
    /// * `target_level` - The level where the result should be placed.
    fn trigger_compaction(
        &mut self,
        sstables: Vec<SSTable<K, V>>,
        target_level: usize,
    ) -> Result<()> {
        let id = self.next_id;
        self.next_id = SSTableId(id.0 + 1);

        let filename = format!("L{}-{}.sst", target_level, id);
        let output_path = self.path.join(&filename);

        let task = CompactionTask::Compact {
            sstables,
            output_path,
            next_id: id,
            target_level,
        };

        self.compaction_tx.send(task).ok();
        Ok(())
    }

    /// Calculates the maximum allowed byte size for a specific level.
    fn level_max_size(&self, level: usize) -> u64 {
        if level == 0 {
            0
        } else {
            BASE_LEVEL_SIZE * (LEVEL_MULTIPLIER.pow((level - 1) as u32) as u64)
        }
    }

    /// Calculates the current total size of all SSTables in a level.
    fn level_current_size(&self, level: usize) -> u64 {
        self.levels
            .get(level)
            .map_or(0, |l| l.iter().map(|s| s.file_size()).sum())
    }

    /// Deletes a key from the database by inserting a tombstone.
    ///
    /// # Arguments
    /// * `key` - The key to delete.
    pub fn delete(&mut self, key: K) -> Result<()> {
        self.handle_compaction_results()?;
        let log_entry = LogEntry::Delete(key.clone());
        self.wal.append(&log_entry)?;
        self.wal.flush()?;
        self.memtable.delete(key);
        Ok(())
    }

    /// Retrieves a value for a given key from the database.
    ///
    /// Searches the MemTable first, then levels of SSTables from newest to oldest.
    ///
    /// # Arguments
    /// * `key` - The key to look up.
    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>> {
        // Search MemTable
        if let Some(entry) = self.memtable.get_entry(key) {
            if entry.is_tombstone {
                return Ok(None);
            }
            return Ok(entry.value.clone());
        }

        // Search SSTables
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

    /// Checks the background worker's result channel and applies any finished compactions.
    fn handle_compaction_results(&mut self) -> Result<()> {
        while let Ok(result) = self.compaction_rx.try_recv() {
            self.apply_compaction_result(result)?;
        }
        Ok(())
    }

    /// Flushes the MemTable to a new Level-0 SSTable.
    fn flush_memtable(&mut self) -> Result<()> {
        let id = self.next_id;
        self.next_id = SSTableId(id.0 + 1);
        let filename = format!("L0-{}.sst", id);
        let sstable_path = self.path.join(&filename);

        let new_sstable = SSTable::write_from_memtable(&sstable_path, &self.memtable, id)?;

        // Update Manifest
        self.manifest.append(&ManifestEntry::AddSSTable {
            level: 0,
            path: PathBuf::from(filename),
        })?;
        self.manifest.append(&ManifestEntry::NextID(self.next_id))?;
        self.manifest.flush()?;

        // Update Memory
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

    /// Atomically applies a successful compaction to the database state.
    ///
    /// This updates the Manifest, synchronizes the memory levels, and cleans up old files.
    fn apply_compaction_result(&mut self, result: CompactionResult<K, V>) -> Result<()> {
        match result {
            CompactionResult::Success {
                sstable,
                level,
                removed_ids,
            } => {
                let mut old_paths = Vec::new();

                // Manifest: Log removals and find physical paths
                for id in &removed_ids {
                    for l in 0..self.levels.len() {
                        if let Some(pos) = self.levels[l].iter().position(|s| s.id() == *id) {
                            let sst = &self.levels[l][pos];
                            if let Some(file_name) = sst.path().file_name() {
                                self.manifest.append(&ManifestEntry::RemoveSSTable {
                                    level: l,
                                    path: PathBuf::from(file_name),
                                })?;
                                old_paths.push(sst.path().to_path_buf());
                            }
                        }
                    }
                }

                // Manifest: Log new file
                if let Some(new_file_name) = sstable.path().file_name() {
                    self.manifest.append(&ManifestEntry::AddSSTable {
                        level,
                        path: PathBuf::from(new_file_name),
                    })?;
                }
                self.manifest.flush()?;

                // Memory: Synchronize levels
                for id in removed_ids {
                    for l in 0..self.levels.len() {
                        self.levels[l].retain(|s| s.id() != id);
                    }
                }

                if level >= self.levels.len() {
                    self.levels.resize_with(level + 1, Vec::new);
                }
                self.levels[level].push(sstable);
                self.levels[level].sort_by_key(|s| s.id());

                // Disk: Cleanup
                for path in old_paths {
                    let _ = std::fs::remove_file(path);
                }
                Ok(())
            }
            CompactionResult::Failure(e) => Err(Error::Corruption(format!(
                "Compaction worker failed: {}",
                e
            ))),
        }
    }
}
