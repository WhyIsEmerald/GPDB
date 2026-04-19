use crate::db::cache::BlockCache;
use crate::db::sstable::SSTableIterator;
use crate::{DBKey, Entry, Result, SSTable, SSTableId};
use serde::{Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use std::sync::Arc;
use std::{cmp::Ordering, collections::BinaryHeap, path::Path};

/// A request sent to the background compaction thread.
pub enum CompactionTask<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Merge the given SSTables into a new one.
    Compact {
        /// The SSTables to be merged.
        sstables: Vec<SSTable<K, V>>,
        /// The file path for the resulting SSTable.
        output_path: PathBuf,
        /// The ID for the resulting SSTable.
        next_id: SSTableId,
        /// The level where the output SSTable will be placed.
        target_level: usize,
        /// Block cache to associate with the new SSTable.
        block_cache: Option<Arc<BlockCache<K, V>>>,
    },
    /// Tell the background thread to stop.
    Shutdown,
}

/// The result of a background compaction task.
pub enum CompactionResult<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Compaction finished successfully.
    Success {
        /// The newly created SSTable.
        sstable: SSTable<K, V>,
        /// The level where the new SSTable was placed.
        level: usize,
        /// The original SSTables that were merged. Returning them allows the DB to clean them up.
        original_sstables: Vec<SSTable<K, V>>,
    },
    /// Compaction failed with an error message.
    Failure(String),
}

pub struct MergeElement<K, V> {
    pub sstable_id: SSTableId,
    pub entry: Entry<K, V>,
    pub iter_index: usize,
}

impl<K: DBKey, V> PartialEq for MergeElement<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key
    }
}

impl<K: DBKey, V> PartialOrd for MergeElement<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: DBKey, V> Eq for MergeElement<K, V> {}

impl<K: DBKey, V> Ord for MergeElement<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.entry.key.cmp(&self.entry.key) {
            Ordering::Equal => self.sstable_id.cmp(&other.sstable_id),
            ord => ord,
        }
    }
}

pub struct MergeStream<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    heap: BinaryHeap<MergeElement<K, V>>,
    iters: Vec<SSTableIterator<K, V>>,
}

impl<K, V> MergeStream<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a new MergeStream from a slice of SSTables.
    ///
    /// # Arguments
    /// * `sstables` - The set of sorted SSTables to be merged and deduplicated.
    pub fn new(sstables: &[SSTable<K, V>]) -> Result<Self> {
        let mut iters = Vec::with_capacity(sstables.len());
        let mut heap = BinaryHeap::with_capacity(sstables.len());

        for (idx, sst) in sstables.iter().enumerate() {
            let mut iter = sst.iter()?;
            if let Some(entry) = iter.next() {
                let entry = entry?;
                heap.push(MergeElement {
                    sstable_id: sst.id(),
                    entry,
                    iter_index: idx,
                });
            }
            iters.push(iter);
        }
        Ok(Self { heap, iters })
    }
}

impl<K, V> Iterator for MergeStream<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let winner = self.heap.pop()?;

        if let Some(result) = self.iters[winner.iter_index].next() {
            match result {
                Ok(entry) => {
                    self.heap.push(MergeElement {
                        sstable_id: winner.sstable_id,
                        entry,
                        iter_index: winner.iter_index,
                    });
                }
                Err(e) => return Some(Err(e)),
            }
        }

        loop {
            if let Some(peeked) = self.heap.peek() {
                if peeked.entry.key != winner.entry.key {
                    break;
                }
            } else {
                break;
            }

            let old = self.heap.pop().unwrap();
            if let Some(result) = self.iters[old.iter_index].next() {
                match result {
                    Ok(entry) => {
                        self.heap.push(MergeElement {
                            sstable_id: old.sstable_id,
                            entry,
                            iter_index: old.iter_index,
                        });
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
        }

        Some(Ok(winner.entry))
    }
}

pub struct Compactor;

impl Compactor {
    /// Finds all SSTables in `target_level` that overlap with the `candidate`.
    ///
    /// # Arguments
    /// * `candidate` - The SSTable whose range we are checking.
    /// * `target_level` - The level of SSTables to check for overlaps.
    pub fn find_overlapping_sstables<K, V>(
        candidate: &SSTable<K, V>,
        target_level: &[SSTable<K, V>],
    ) -> Vec<usize>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let mut overlapping = Vec::new();
        for (idx, sst) in target_level.iter().enumerate() {
            if candidate.overlaps(sst) {
                overlapping.push(idx);
            }
        }
        overlapping
    }

    /// Finds all SSTables in `target_level` that overlap with the range defined by a set of `sstables`.
    ///
    /// # Arguments
    /// * `sstables` - The source SSTables defining the key range.
    /// * `target_level` - The level to search for overlapping files.
    pub fn find_range_overlapping_sstables<K, V>(
        sstables: &[SSTable<K, V>],
        target_level: &[SSTable<K, V>],
    ) -> Vec<usize>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if sstables.is_empty() {
            return Vec::new();
        }

        let min = sstables.iter().map(|s| s.min_key()).min().unwrap();
        let max = sstables.iter().map(|s| s.max_key()).max().unwrap();

        let mut overlapping = Vec::new();
        for (idx, sst) in target_level.iter().enumerate() {
            if sst.overlaps_range(min, max) {
                overlapping.push(idx);
            }
        }
        overlapping
    }

    /// Merges a slice of SSTables into a single new SSTable at the given path.
    ///
    /// # Arguments
    /// * `sstables` - The input SSTables to be merged.
    /// * `output_path` - The filesystem path where the new SSTable will be written.
    /// * `new_id` - The unique ID to assign to the new SSTable.
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
        let target_level = 1; // Default for compacting L0 to disk
        SSTable::write_from_iter(output_path, stream, new_id, target_level, block_cache)
    }

    /// Merges a slice of L0 SSTables into a single new L1 SSTable.
    /// (Maintained for backwards compatibility, redirects to generic compact)
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

    /// The main entry point for the background worker thread.
    ///
    /// # Arguments
    /// * `receiver` - The channel endpoint to receive new compaction tasks.
    /// * `sender` - The channel endpoint to report completion or failure back to the DB.
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
                CompactionTask::Shutdown => {
                    break;
                }
            }
        }
    }
}
