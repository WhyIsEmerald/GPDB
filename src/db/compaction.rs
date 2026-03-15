use crate::{DBKey, Entry, SSTableId};
use std::cmp::Ordering;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueEntry;
    use std::collections::BinaryHeap;
    use std::sync::Arc;

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
}
