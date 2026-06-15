pub mod overlap;
pub mod stream;

use crate::db::cache::BlockCache;
use crate::db::compaction::stream::MergeStream;
use crate::{DBKey, Result, SSTable, SSTableId};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A request sent to the background compaction thread.
pub enum CompactionTask<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The compact operation used for merging SSTables.
    Compact {
        /// The SSTables used for compaction.
        sstables: Vec<SSTable<K, V>>,
        /// The output path used for the new SSTable.
        output_path: PathBuf,
        /// The new SSTable ID used for the result.
        next_id: SSTableId,
        /// The target level used for the new SSTable.
        target_level: usize,
        /// The block cache used for the new SSTable.
        block_cache: Option<Arc<BlockCache<K, V>>>,
    },
    /// The shutdown operation used to stop the worker.
    Shutdown,
}

/// A struct that represents the result of a background compaction task.
pub enum CompactionResult<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The success result used for a successful compaction.
    Success {
        /// The new SSTable created.
        sstable: SSTable<K, V>,
        /// The level where the new SSTable was placed.
        level: usize,
        /// The original SSTables that were merged.
        original_sstables: Vec<SSTable<K, V>>,
    },
    /// The failure result used for a failed compaction.
    Failure {
        /// The error message used for the failure.
        error: String,
        /// The original SSTables that were involved in the failed compaction.
        original_sstables: Vec<SSTable<K, V>>,
    },
}

/// A struct that provides functionality for compacting SSTables.
pub struct Compactor;

impl Compactor {
    /// Compacts a set of SSTables into a new one.
    pub fn compact<K, V>(
        sstables: &[SSTable<K, V>],
        output_path: &Path,
        new_id: SSTableId,
        target_level: usize,
        block_cache: Option<Arc<BlockCache<K, V>>>,
    ) -> Result<SSTable<K, V>>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let stream = MergeStream::new(sstables)?;

        if target_level == crate::db::database::MAX_LEVEL {
            let filtered_stream = stream.filter(|res| {
                if let Ok(entry) = res {
                    !entry.value.is_tombstone
                } else {
                    true
                }
            });
            SSTable::write_from_iter(
                output_path,
                filtered_stream,
                new_id,
                target_level,
                block_cache,
            )
        } else {
            SSTable::write_from_iter(output_path, stream, new_id, target_level, block_cache)
        }
    }

    /// Compacts L0 SSTables.
    pub fn compact_l0<K, V>(
        sstables: &[SSTable<K, V>],
        output_path: &Path,
        new_id: SSTableId,
        block_cache: Option<Arc<BlockCache<K, V>>>,
    ) -> Result<SSTable<K, V>>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        Self::compact(sstables, output_path, new_id, 0, block_cache)
    }

    /// Runs the compaction worker thread.
    pub fn run_worker<K, V>(
        receiver: std::sync::mpsc::Receiver<CompactionTask<K, V>>,
        sender: std::sync::mpsc::Sender<CompactionResult<K, V>>,
    ) where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        while let Ok(task) = receiver.recv() {
            match task {
                CompactionTask::Compact {
                    sstables,
                    output_path,
                    next_id,
                    target_level,
                    block_cache,
                } => {
                    let result =
                        Self::compact(&sstables, &output_path, next_id, target_level, block_cache);
                    match result {
                        Ok(sstable) => {
                            sender
                                .send(CompactionResult::Success {
                                    sstable,
                                    level: target_level,
                                    original_sstables: sstables,
                                })
                                .ok();
                        }
                        Err(e) => {
                            sender
                                .send(CompactionResult::Failure {
                                    error: e.to_string(),
                                    original_sstables: sstables,
                                })
                                .ok();
                        }
                    }
                }
                CompactionTask::Shutdown => break,
            }
        }
    }
}
