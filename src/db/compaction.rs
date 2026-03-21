use crate::db::sstable::SSTableIterator;
use crate::{DBKey, Entry, Result, SSTable, SSTableId};
use serde::{Serialize, de::DeserializeOwned};
use std::{cmp::Ordering, collections::BinaryHeap, path::Path};

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
        // Rust's BinaryHeap is a Max-Heap.
        // To make it a Min-Heap for keys, we compare other to self.
        match other.entry.key.cmp(&self.entry.key) {
            Ordering::Equal => {
                // If keys are equal, we want the highest ID to be "greater"
                // so it pops off the Max-Heap first.
                self.sstable_id.cmp(&other.sstable_id)
            }
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
    // Create a new MergeStream from a slice of SSTables.
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
    /// Merges a slice of L0 SSTables into a single new L1 SSTable.
    pub fn compact_l0<K, V>(
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
        heap.push(make_el("C", 1, 0));
        heap.push(make_el("A", 1, 0));
        heap.push(make_el("B", 1, 0));

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
