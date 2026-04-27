use crate::db::database::DB;
use crate::db::database::VersionState;
use crate::types::records::DBKey;
use crate::{ManifestEntry, MemTable, Result, SSTable, SSTableId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub(crate) fn switch_memtable(&self) -> Result<()> {
        let _lock = self.flush_mutex.lock();
        if self.config.memtable_size.load(Ordering::Relaxed) < self.config.max_memtable_size {
            return Ok(());
        }

        let old_memtable = self.memtable.load_full();
        let new_memtable = Arc::new(MemTable::new());

        // Rotate WAL before switching memtable
        let wal_id = self.wal.rotate()?;

        self.memtable.store(new_memtable);
        self.config.memtable_size.store(0, Ordering::Relaxed);

        {
            let _manifest = self.manifest.lock();
            let old_version = self.version.load();
            let mut new_immutables = old_version.immutables.clone();
            new_immutables.push(crate::db::database::ImmutableMemTable {
                memtable: old_memtable,
                wal_id,
            });

            self.version.store(Arc::new(VersionState {
                levels: old_version.levels.clone(),
                immutables: new_immutables,
            }));
        }

        self.flush_immutables()?;
        Ok(())
    }

    pub(crate) fn flush_immutables(&self) -> Result<()> {
        loop {
            let version = self.version.load();
            if version.immutables.is_empty() {
                break;
            }

            let imm_entry = version.immutables[0].clone();
            let imm = Arc::clone(&imm_entry.memtable);

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
                let state = self.compaction_state.lock();
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
                new_immutables.remove(0);

                self.version.store(Arc::new(VersionState {
                    levels: new_levels,
                    immutables: new_immutables,
                }));
            }

            {
                self.wal.delete(imm_entry.wal_id)?;
            }
        }
        self.check_all_compactions();
        Ok(())
    }
}
