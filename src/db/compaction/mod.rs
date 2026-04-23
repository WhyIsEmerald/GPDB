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
    Compact {
        sstables: Vec<SSTable<K, V>>,
        output_path: PathBuf,
        next_id: SSTableId,
        target_level: usize,
        block_cache: Option<Arc<BlockCache<K, V>>>,
    },
    Shutdown,
}

/// The result of a background compaction task.
pub enum CompactionResult<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Success {
        sstable: SSTable<K, V>,
        level: usize,
        original_sstables: Vec<SSTable<K, V>>,
    },
    Failure(String),
}

pub struct Compactor;

impl Compactor {
    pub fn compact<K, V>(
        sstables: &[SSTable<K, V>],
        output_path: &Path,
        new_id: SSTableId,
        block_cache: Option<Arc<BlockCache<K, V>>>,
    ) -> Result<SSTable<K, V>>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let stream = MergeStream::new(sstables)?;
        let target_level = 1;
        SSTable::write_from_iter(output_path, stream, new_id, target_level, block_cache)
    }

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
        Self::compact(sstables, output_path, new_id, block_cache)
    }

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
                    let result = Self::compact(&sstables, &output_path, next_id, block_cache);
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
                            sender.send(CompactionResult::Failure(e.to_string())).ok();
                        }
                    }
                }
                CompactionTask::Shutdown => break,
            }
        }
    }
}
