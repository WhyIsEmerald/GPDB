use crate::db::compaction::{CompactionResult, CompactionTask, Compactor};
use crate::db::version::VersionState;
use crate::{
    BlockCache, DBKey, LogEntry, Manifest, ManifestEntry, MemTable, Result, SSTable, SSTableId,
    Wal, WriteBatch,
};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

pub(crate) const MANIFEST_FILE_NAME: &str = "MANIFEST";
pub(crate) const WAL_FILE_NAME: &str = "wal.log";

/// The public handle to the database.
#[derive(Debug, Clone)]
pub struct DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub(crate) memtable: Arc<ArcSwap<MemTable<K, V>>>,
    pub(crate) wal: Arc<Mutex<Wal<K, V>>>,
    pub(crate) manifest: Arc<Mutex<Manifest>>,
    pub(crate) version: Arc<ArcSwap<VersionState<K, V>>>,
    pub(crate) block_cache: Arc<BlockCache<K, V>>,
    pub(crate) compaction_state: Arc<Mutex<CompactionState<K, V>>>,
    pub(crate) flush_mutex: Arc<Mutex<()>>,
    pub(crate) config: Arc<DBConfig<K, V>>,
}

#[derive(Debug)]
pub(crate) struct DBConfig<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub(crate) path: PathBuf,
    pub(crate) max_memtable_size: usize,
    pub(crate) memtable_size: AtomicUsize,
    pub(crate) compaction_tx: mpsc::Sender<CompactionTask<K, V>>,
}

#[derive(Debug)]
pub(crate) struct CompactionState<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub(crate) next_id: SSTableId,
    pub(crate) compacting_ids: HashSet<SSTableId>,
    pub(crate) compaction_rx: mpsc::Receiver<CompactionResult<K, V>>,
}

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub fn open(path: &Path, max_memtable_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let block_cache = Arc::new(BlockCache::new(32 * 1024 * 1024));

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
            match record_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
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
            let sstable = SSTable::open(&path.join(rel_path), Some(Arc::clone(&block_cache)))?;
            levels[level].push(sstable);
        }
        for l in &mut levels {
            l.sort_by_key(|sst| sst.id());
        }

        let version = Arc::new(ArcSwap::from(Arc::new(VersionState {
            levels,
            immutables: Vec::new(),
        })));
        let wal_path = path.join(WAL_FILE_NAME);
        let memtable = Arc::new(MemTable::new());
        let wal = if wal_path.exists() {
            let existing_wal = Wal::open(&wal_path)?;
            for entry in existing_wal.iter()? {
                match entry.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
                    LogEntry::Put(k, v) => memtable.put(k, v),
                    LogEntry::Delete(k) => memtable.delete(k),
                }
            }
            Wal::open(&wal_path)?
        } else {
            Wal::create(&wal_path)?
        };

        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        std::thread::spawn(move || {
            Compactor::run_worker::<K, V>(task_rx, result_tx);
        });

        Ok(Self {
            memtable: Arc::new(ArcSwap::from(memtable)),
            wal: Arc::new(Mutex::new(wal)),
            manifest: Arc::new(Mutex::new(manifest)),
            version,
            block_cache,
            compaction_state: Arc::new(Mutex::new(CompactionState {
                next_id,
                compacting_ids: HashSet::new(),
                compaction_rx: result_rx,
            })),
            flush_mutex: Arc::new(Mutex::new(())),
            config: Arc::new(DBConfig {
                path: path.to_path_buf(),
                max_memtable_size,
                memtable_size: AtomicUsize::new(0),
                compaction_tx: task_tx,
            }),
        })
    }

    pub fn put(&self, key: K, value: V) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch(batch)
    }

    pub fn delete(&self, key: K) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch(batch)
    }

    pub fn write_batch(&self, batch: WriteBatch<K, V>) -> Result<()> {
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
                    entry.value.value.clone().expect("Value missing"),
                )
            };
            log_entries.push(log_entry);
        }

        {
            let mut wal = self.wal.lock();
            wal.append_batch(&log_entries)?;
            wal.flush()?;
        }

        let memtable = self.memtable.load();
        for entry in log_entries {
            match entry {
                LogEntry::Put(k, v) => memtable.put(k, v),
                LogEntry::Delete(k) => memtable.delete(k),
            }
        }

        if self
            .config
            .memtable_size
            .fetch_add(total_batch_size, Ordering::Relaxed)
            + total_batch_size
            >= self.config.max_memtable_size
        {
            self.switch_memtable()?;
        }
        Ok(())
    }

    fn switch_memtable(&self) -> Result<()> {
        let _lock = self.flush_mutex.lock();
        if self.config.memtable_size.load(Ordering::Relaxed) < self.config.max_memtable_size {
            return Ok(());
        }

        let old_memtable = self.memtable.load_full();
        let new_memtable = Arc::new(MemTable::new());

        // 1. Swap active memtable
        self.memtable.store(new_memtable);
        self.config.memtable_size.store(0, Ordering::Relaxed);

        // 2. Add old to immutables list in version
        {
            let _manifest = self.manifest.lock(); // Serialize version update
            let old_version = self.version.load();
            let mut new_immutables = old_version.immutables.clone();
            new_immutables.push(old_memtable);

            self.version.store(Arc::new(VersionState {
                levels: old_version.levels.clone(),
                immutables: new_immutables,
            }));
        }

        // 3. Trigger flush in background or here?
        // For now, flush here while holding lock to keep it simple.
        self.flush_immutables()?;
        Ok(())
    }

    fn flush_immutables(&self) -> Result<()> {
        // flush_mutex is already held by switch_memtable or caller
        loop {
            let version = self.version.load();
            if version.immutables.is_empty() {
                break;
            }

            let imm = Arc::clone(&version.immutables[0]);

            let id: SSTableId;
            let filename: String;
            let path: PathBuf;

            {
                let mut state = self.compaction_state.lock();
                id = state.next_id;
                state.next_id = SSTableId(id.0 + 1);
                filename = format!("L0-{}.sst", id);
                path = self.config.path.join(&filename);
            }

            let new_sstable =
                SSTable::write_from_memtable(&path, &imm, id, Some(Arc::clone(&self.block_cache)))?;

            {
                let mut manifest = self.manifest.lock();
                let mut state = self.compaction_state.lock();
                manifest.append(&ManifestEntry::AddSSTable {
                    level: 0,
                    path: PathBuf::from(&filename),
                })?;
                manifest.append(&ManifestEntry::NextID(state.next_id))?;
                manifest.flush()?;

                let old_version = self.version.load();
                let mut new_levels = old_version.levels.clone();
                new_levels[0].push(new_sstable);
                new_levels[0].sort_by_key(|sst| sst.id());

                let mut new_immutables = old_version.immutables.clone();
                new_immutables.remove(0); // Pop the one we just flushed

                self.version.store(Arc::new(VersionState {
                    levels: new_levels,
                    immutables: new_immutables,
                }));
            }

            {
                let mut wal = self.wal.lock();
                wal.clear()?;
            }
        }
        self.check_all_compactions();
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>> {
        // 1. Check active MemTable
        if let Some(entry) = self.memtable.load().get_entry(key) {
            if entry.is_tombstone {
                return Ok(None);
            }
            return Ok(entry.value);
        }

        // 2. Check Immutable MemTables and SSTables
        let version = self.version.load();

        // Search immutables in reverse (newest first)
        for imm in version.immutables.iter().rev() {
            if let Some(entry) = imm.get_entry(key) {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(entry.value);
            }
        }

        for level in &version.levels {
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

    pub fn handle_compaction_results(&self) -> Result<()> {
        let mut results = Vec::new();
        {
            let state = self.compaction_state.lock();
            while let Ok(result) = state.compaction_rx.try_recv() {
                results.push(result);
            }
        }
        if results.is_empty() {
            return Ok(());
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

    fn check_all_compactions(&self) {
        let version = self.version.load();
        for level in 0..version.levels.len() {
            self.maybe_trigger_compaction(level);
        }
    }

    fn maybe_trigger_compaction(&self, level: usize) {
        let threshold = 4;
        let version = self.version.load();
        if version.levels[level].len() < threshold {
            return;
        }

        let mut state = self.compaction_state.lock();
        let any_compacting = version.levels[level]
            .iter()
            .any(|s| state.compacting_ids.contains(&s.id()));
        if !any_compacting {
            let sstables = version.levels[level].clone();
            self.trigger_compaction(&mut state, sstables, level + 1);
        }
    }

    fn trigger_compaction(
        &self,
        state: &mut CompactionState<K, V>,
        sstables: Vec<SSTable<K, V>>,
        target_level: usize,
    ) {
        for sst in &sstables {
            state.compacting_ids.insert(sst.id());
        }
        let id = state.next_id;
        state.next_id = SSTableId(id.0 + 1);
        let output_path = self
            .config
            .path
            .join(format!("L{}-{}.sst", target_level, id));
        let _ = self.config.compaction_tx.send(CompactionTask::Compact {
            sstables,
            output_path,
            next_id: id,
            target_level,
            block_cache: Some(Arc::clone(&self.block_cache)),
        });
    }

    fn apply_compaction_success(
        &self,
        mut sstable: SSTable<K, V>,
        level: usize,
        original_sstables: Vec<SSTable<K, V>>,
    ) -> Result<()> {
        sstable.set_cache(Arc::clone(&self.block_cache));
        let removed_ids: HashSet<SSTableId> = original_sstables.iter().map(|s| s.id()).collect();

        {
            let mut manifest = self.manifest.lock();
            let mut state = self.compaction_state.lock();

            let old_version = self.version.load();
            let mut new_levels = old_version.levels.clone();

            for sst in &original_sstables {
                state.compacting_ids.remove(&sst.id());
                let mut source_level = 0;
                for (l_idx, level_vec) in new_levels.iter().enumerate() {
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

            for l in 0..new_levels.len() {
                new_levels[l].retain(|s| !removed_ids.contains(&s.id()));
            }
            if level >= new_levels.len() {
                new_levels.resize_with(level + 1, Vec::new);
            }
            new_levels[level].push(sstable);
            new_levels[level].sort_by_key(|s| s.id());

            self.version.store(Arc::new(VersionState {
                levels: new_levels,
                immutables: old_version.immutables.clone(),
            }));
        }
        Ok(())
    }

    pub fn compaction_backlog(&self) -> usize {
        let state = self.compaction_state.lock();
        state.compacting_ids.len()
    }

    pub fn total_sst_count(&self) -> usize {
        let version = self.version.load();
        version.levels.iter().map(|l| l.len()).sum()
    }
}
