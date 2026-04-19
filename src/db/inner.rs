use crate::db::compaction::{CompactionResult, CompactionTask};
use crate::{
    BlockCache, DBKey, LogEntry, Manifest, ManifestEntry, MemTable, Result, SSTable, SSTableId, Wal, WriteBatch,
};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc;

pub(crate) const MANIFEST_FILE_NAME: &str = "MANIFEST";
pub(crate) const WAL_FILE_NAME: &str = "wal.log";

pub struct DBInner<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub(crate) path: PathBuf,
    pub(crate) memtable: MemTable<K, V>,
    pub(crate) wal: Wal<K, V>,
    pub(crate) manifest: parking_lot::Mutex<Manifest>,
    pub(crate) levels: Vec<Vec<SSTable<K, V>>>,
    pub(crate) next_id: SSTableId,
    pub(crate) max_memtable_size: usize,
    pub(crate) memtable_size: usize,
    pub(crate) block_cache: Arc<BlockCache<K, V>>,
    pub(crate) compaction_tx: mpsc::Sender<CompactionTask<K, V>>,
    pub(crate) compaction_rx: parking_lot::Mutex<mpsc::Receiver<CompactionResult<K, V>>>,
    pub(crate) compacting_ids: HashSet<SSTableId>,
}

impl<K, V> std::fmt::Debug for DBInner<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBInner")
            .field("path", &self.path)
            .field("levels_count", &self.levels.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl<K, V> DBInner<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    /// Returns the number of SSTables currently in the compaction pipeline.
    pub fn compaction_backlog(&self) -> usize {
        self.compacting_ids.len()
    }

    /// Returns the total number of SSTables across all levels.
    pub fn total_sst_count(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch(batch)
    }

    pub fn delete(&mut self, key: K) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch(batch)
    }

    pub fn write_batch(&mut self, batch: WriteBatch<K, V>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        self.handle_compaction_results()?;

        let mut log_entries = Vec::with_capacity(batch.entries.len());
        let mut total_batch_size = 0;

        for entry in batch.entries {
            let key_size = std::mem::size_of_val(&entry.key);
            let val_size = entry
                .value
                .value
                .as_ref()
                .map_or(0, |v| std::mem::size_of_val(&**v));
            total_batch_size += key_size + val_size;

            let log_entry = if entry.value.is_tombstone {
                LogEntry::Delete(entry.key.clone())
            } else {
                LogEntry::Put(
                    entry.key.clone(),
                    entry.value.value.clone().expect("Value missing in put"),
                )
            };
            log_entries.push(log_entry);
        }

        self.wal.append_batch(&log_entries)?;
        self.wal.flush()?;

        for entry in log_entries {
            match entry {
                LogEntry::Put(k, v) => self.memtable.put(k, v),
                LogEntry::Delete(k) => self.memtable.delete(k),
            };
        }

        self.memtable_size += total_batch_size;
        if self.memtable_size >= self.max_memtable_size {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>> {
        if let Some(entry) = self.memtable.get_entry(key) {
            if entry.is_tombstone {
                return Ok(None);
            }
            return Ok(entry.value.clone());
        }

        for level in &self.levels {
            for sstable in level.iter().rev() {
                if let Some(val_entry) = sstable.get(key)? {
                    if val_entry.is_tombstone {
                        return Ok(None);
                    }
                    return Ok(val_entry.value);
                }
            }
        }
        Ok(None)
    }

    pub fn handle_compaction_results(&mut self) -> Result<()> {
        let mut results = Vec::new();
        {
            let rx = self.compaction_rx.lock();
            while let Ok(result) = rx.try_recv() {
                results.push(result);
            }
        }

        for result in results {
            match result {
                CompactionResult::Success {
                    sstable,
                    level,
                    original_sstables,
                } => {
                    self.apply_compaction_success(sstable, level, original_sstables)?;
                }
                CompactionResult::Failure(e) => {
                    eprintln!("Compaction worker failed: {}", e);
                }
            }
        }

        self.check_all_compactions();
        Ok(())
    }

    /// Scans all levels and triggers any needed compaction tasks.
    fn check_all_compactions(&mut self) {
        for level in 0..self.levels.len() {
            self.maybe_trigger_compaction(level);
        }
    }

    fn maybe_trigger_compaction(&mut self, level: usize) {
        let threshold = 4;
        if self.levels[level].len() < threshold {
            return;
        }

        let any_compacting = self.levels[level]
            .iter()
            .any(|s| self.compacting_ids.contains(&s.id()));
        if !any_compacting {
            let sstables = self.levels[level].clone();
            self.trigger_compaction(sstables, level + 1);
        }
    }

    fn trigger_compaction(&mut self, sstables: Vec<SSTable<K, V>>, target_level: usize) {
        for sst in &sstables {
            self.compacting_ids.insert(sst.id());
        }

        let id = self.next_id;
        self.next_id = SSTableId(id.0 + 1);
        let output_path = self.path.join(format!("L{}-{}.sst", target_level, id));

        let _ = self.compaction_tx.send(CompactionTask::Compact {
            sstables,
            output_path,
            next_id: id,
            target_level,
            block_cache: Some(Arc::clone(&self.block_cache)),
        });
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let id = self.next_id;
        self.next_id = SSTableId(id.0 + 1);
        let filename = format!("L0-{}.sst", id);
        let path = self.path.join(&filename);

        let new_sstable =
            SSTable::write_from_memtable(&path, &self.memtable, id, Some(Arc::clone(&self.block_cache)))?;

        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestEntry::AddSSTable {
                level: 0,
                path: PathBuf::from(filename),
            })?;
            manifest.append(&ManifestEntry::NextID(self.next_id))?;
            manifest.flush()?;
        }

        self.levels[0].push(new_sstable);
        self.levels[0].sort_by_key(|sst| sst.id());

        self.memtable.clear();
        self.memtable_size = 0;
        self.wal.clear()?;

        self.check_all_compactions();
        Ok(())
    }

    fn apply_compaction_success(
        &mut self,
        mut sstable: SSTable<K, V>,
        level: usize,
        original_sstables: Vec<SSTable<K, V>>,
    ) -> Result<()> {
        sstable.set_cache(Arc::clone(&self.block_cache));
        let removed_ids: HashSet<SSTableId> = original_sstables.iter().map(|s| s.id()).collect();

        {
            let mut manifest = self.manifest.lock();
            for sst in &original_sstables {
                self.compacting_ids.remove(&sst.id());

                let mut source_level = 0;
                for (l_idx, level_vec) in self.levels.iter().enumerate() {
                    if level_vec.iter().any(|s| s.id() == sst.id()) {
                        source_level = l_idx;
                        break;
                    }
                }

                if let Some(file_name) = sst.path().file_name() {
                    manifest.append(&ManifestEntry::RemoveSSTable {
                        level: source_level,
                        path: PathBuf::from(file_name),
                    })?;
                }
                let _ = std::fs::remove_file(sst.path());
            }

            if let Some(new_file_name) = sstable.path().file_name() {
                manifest.append(&ManifestEntry::AddSSTable {
                    level,
                    path: PathBuf::from(new_file_name),
                })?;
            }
            manifest.flush()?;
        }

        for l in 0..self.levels.len() {
            self.levels[l].retain(|s| !removed_ids.contains(&s.id()));
        }

        if level >= self.levels.len() {
            self.levels.resize_with(level + 1, Vec::new);
        }
        self.levels[level].push(sstable);
        self.levels[level].sort_by_key(|s| s.id());

        Ok(())
    }
}
