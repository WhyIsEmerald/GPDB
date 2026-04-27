pub mod flush;
pub mod read;
pub mod write;

use crate::db::compaction::{CompactionResult, CompactionTask, Compactor};
use crate::db::wal::WalManager;
use crate::{
    BlockCache, DBKey, LogEntry, Manifest, ManifestEntry, MemTable, Result, SSTable, SSTableId, Wal,
};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;

pub(crate) const MANIFEST_FILE_NAME: &str = "MANIFEST";

/// An immutable point-in-time view of the database's SSTables and Immutable MemTables.
#[derive(Debug)]
pub struct VersionState<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub levels: Vec<Vec<SSTable<K, V>>>,
    pub immutables: Vec<ImmutableMemTable<K, V>>,
}

#[derive(Debug)]
pub struct ImmutableMemTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub memtable: Arc<MemTable<K, V>>,
    pub wal_id: u64,
}

impl<K, V> Clone for ImmutableMemTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            memtable: Arc::clone(&self.memtable),
            wal_id: self.wal_id,
        }
    }
}

impl<K, V> VersionState<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(
        levels: Vec<Vec<SSTable<K, V>>>,
        immutables: Vec<ImmutableMemTable<K, V>>,
    ) -> Self {
        Self { levels, immutables }
    }
}

/// The public handle to the database.
#[derive(Debug, Clone)]
pub struct DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub(crate) memtable: Arc<ArcSwap<MemTable<K, V>>>,
    pub(crate) wal: Arc<WalManager<K, V>>,
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

        // Discover and recover WALs
        let mut wal_files = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = name.parse::<u64>() {
                        wal_files.push((id, path));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        let memtable = Arc::new(MemTable::new());
        let mut last_wal_id = 0;

        for (id, wal_path) in &wal_files {
            last_wal_id = *id;
            let existing_wal = Wal::open(wal_path)?;
            for entry in existing_wal.iter()? {
                match entry.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
                    LogEntry::Put(k, v) => memtable.put(k, v),
                    LogEntry::Delete(k) => memtable.delete(k),
                }
            }
        }

        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        std::thread::spawn(move || {
            Compactor::run_worker::<K, V>(task_rx, result_tx);
        });

        Ok(Self {
            memtable: Arc::new(ArcSwap::from(memtable)),
            wal: Arc::new(WalManager::new(path.to_path_buf(), last_wal_id)?),
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
