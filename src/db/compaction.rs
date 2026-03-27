use crate::db::sstable::SSTableIterator;
use crate::{DBKey, Entry, Result, SSTable, SSTableId};
use serde::{Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use std::{cmp::Ordering, collections::BinaryHeap, path::Path};

/// A request sent to the background compaction thread.
pub enum CompactionTask<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
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
    },
    /// Tell the background thread to stop.
    Shutdown,
}

/// The result of a background compaction task.
pub enum CompactionResult<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    /// Compaction finished successfully.
    Success {
        /// The newly created SSTable.
        sstable: SSTable<K, V>,
        /// The level where the new SSTable was placed.
        level: usize,
        /// The IDs of the SSTables that were merged and should be removed from memory.
        removed_ids: Vec<SSTableId>,
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
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    heap: BinaryHeap<MergeElement<K, V>>,
    iters: Vec<SSTableIterator<K, V>>,
}

impl<K, V> MergeStream<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
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
    K: DBKey,
    V: Serialize + DeserializeOwned,
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
        K: DBKey,
        V: Serialize + DeserializeOwned,
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
        K: DBKey,
        V: Serialize + DeserializeOwned,
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
    ) -> Result<SSTable<K, V>>
    where
        K: DBKey,
        V: Serialize + DeserializeOwned,
    {
        let stream = MergeStream::new(sstables)?;
        SSTable::write_from_iter(output_path, stream, new_id)
    }

    /// Merges a slice of L0 SSTables into a single new L1 SSTable.
    /// (Maintained for backwards compatibility, redirects to generic compact)
    pub fn compact_l0<K, V>(
        sstables: &[SSTable<K, V>],
        output_path: &Path,
        new_id: SSTableId,
    ) -> Result<SSTable<K, V>>
    where
        K: DBKey,
        V: Serialize + DeserializeOwned,
    {
        Self::compact(sstables, output_path, new_id)
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
        K: DBKey + Send + 'static,
        V: Serialize + DeserializeOwned + Send + 'static,
    {
        while let Ok(task) = receiver.recv() {
            match task {
                CompactionTask::Compact {
                    sstables,
                    output_path,
                    next_id,
                    target_level,
                } => {
                    // Extract IDs before we move/merge them
                    let removed_ids: Vec<SSTableId> = sstables.iter().map(|s| s.id()).collect();

                    let result = Self::compact(&sstables, &output_path, next_id);
                    match result {
                        Ok(sstable) => {
                            sender
                                .send(CompactionResult::Success {
                                    sstable,
                                    level: target_level,
                                    removed_ids,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemTable, ValueEntry};
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper to create a MergeElement for testing
    fn make_el(key: &str, id: u64, iter_idx: usize) -> MergeElement<String, String> {
        MergeElement {
            sstable_id: SSTableId(id),
            entry: Entry {
                key: key.to_string(),
                value: ValueEntry {
                    value: Some(Arc::new("val".to_string())),
                    is_tombstone: false,
                },
            },
            iter_index: iter_idx,
        }
    }

    #[test]
    fn test_key_priority() {
        let mut heap = BinaryHeap::new();

        // Push keys in "wrong" order
        heap.push(make_el("C", 1, 0));
        heap.push(make_el("A", 1, 0));
        heap.push(make_el("B", 1, 0));

        // BinaryHeap should pop smallest key first because of our flipped Ord
        assert_eq!(heap.pop().unwrap().entry.key, "A");
        assert_eq!(heap.pop().unwrap().entry.key, "B");
        assert_eq!(heap.pop().unwrap().entry.key, "C");
    }

    #[test]
    fn test_id_priority_on_tie() {
        let mut heap = BinaryHeap::new();

        // Three versions of key "A" with different SSTable IDs
        heap.push(make_el("A", 10, 0)); // Newest
        heap.push(make_el("A", 2, 0)); // Oldest
        heap.push(make_el("A", 5, 0)); // Middle

        // For the same key, the HIGHEST ID must come out first
        let winner = heap.pop().unwrap();
        assert_eq!(winner.entry.key, "A");
        assert_eq!(winner.sstable_id, SSTableId(10));

        assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(5));
        assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(2));
    }

    #[test]
    fn test_complex_mix() {
        let mut heap = BinaryHeap::new();

        heap.push(make_el("B", 100, 0));
        heap.push(make_el("A", 1, 0));
        heap.push(make_el("B", 50, 0));
        heap.push(make_el("A", 50, 0));

        // 1. Smallest Key ("A") wins. Between "A"s, highest ID (50) wins.
        let first = heap.pop().unwrap();
        assert_eq!(first.entry.key, "A");
        assert_eq!(first.sstable_id, SSTableId(50));

        // 2. Remaining "A" (ID 1)
        assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(1));

        // 3. Next key ("B"). Highest ID (100) wins.
        assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(100));
        assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(50));
    }

    #[test]
    fn test_merge_stream_integration() {
        let tmp_dir = TempDir::new().unwrap();

        // SST1 (Older): [A, C, E]
        let sst1_path = tmp_dir.path().join("L0-1.sst");
        let mut mem1 = MemTable::new();
        mem1.put("A".to_string(), Arc::new("v1-old".to_string()));
        mem1.put("C".to_string(), Arc::new("v1".to_string()));
        mem1.put("E".to_string(), Arc::new("v1".to_string()));
        let sst1 = SSTable::write_from_memtable(&sst1_path, &mem1, SSTableId(1)).unwrap();

        // SST2 (Newer): [A, B, D]
        let sst2_path = tmp_dir.path().join("L0-2.sst");
        let mut mem2 = MemTable::new();
        mem2.put("A".to_string(), Arc::new("v2-new".to_string()));
        mem2.put("B".to_string(), Arc::new("v2".to_string()));
        mem2.put("D".to_string(), Arc::new("v2".to_string()));
        let sst2 = SSTable::write_from_memtable(&sst2_path, &mem2, SSTableId(2)).unwrap();

        let sstables = vec![sst1, sst2];
        let stream = MergeStream::new(&sstables).unwrap();

        let results: Vec<Entry<String, String>> = stream.map(|r| r.unwrap()).collect();

        // Check deduplication (A should be v2-new)
        assert_eq!(results[0].key, "A");
        assert_eq!(results[0].value.value.as_ref().unwrap().as_str(), "v2-new");

        // Check sorting and inclusion
        assert_eq!(results[1].key, "B");
        assert_eq!(results[2].key, "C");
        assert_eq!(results[3].key, "D");
        assert_eq!(results[4].key, "E");
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_compactor_l0_to_disk() {
        let tmp_dir = TempDir::new().unwrap();

        // Create L0-1 [A, C]
        let path1 = tmp_dir.path().join("L0-1.sst");
        let mut mem1 = MemTable::new();
        mem1.put("A".to_string(), Arc::new("old".to_string()));
        mem1.put("C".to_string(), Arc::new("val".to_string()));
        let sst1 = SSTable::write_from_memtable(&path1, &mem1, SSTableId(1)).unwrap();

        // L0-2 [A, B]
        let path2 = tmp_dir.path().join("L0-2.sst");
        let mut mem2 = MemTable::new();
        mem2.put("A".to_string(), Arc::new("new".to_string()));
        mem2.put("B".to_string(), Arc::new("val".to_string()));
        let sst2 = SSTable::write_from_memtable(&path2, &mem2, SSTableId(2)).unwrap();

        // Compact them into L1-5
        let l1_path = tmp_dir.path().join("L1-5.sst");
        let sst_l1 = Compactor::compact_l0(&[sst1, sst2], &l1_path, SSTableId(5))
            .expect("Compaction failed");

        // Verify the new SSTable on disk
        assert_eq!(sst_l1.id(), SSTableId(5));
        assert_eq!(sst_l1.len(), 3); // A, B, C

        let val_a = sst_l1.get(&"A".to_string()).unwrap().unwrap();
        assert_eq!(val_a.value.unwrap().as_str(), "new");

        let val_b = sst_l1.get(&"B".to_string()).unwrap().unwrap();
        assert!(val_b.value.is_some());

        let val_c = sst_l1.get(&"C".to_string()).unwrap().unwrap();
        assert!(val_c.value.is_some());
    }
}
