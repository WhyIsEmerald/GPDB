use crate::db::sstable::SSTableIterator;
use crate::{DBKey, Entry, Result, SSTable, SSTableId};
use serde::{Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A struct that represents an element in the merge stream, associating an entry with its source SSTable.
pub struct MergeElement<K, V> {
    /// The SSTable ID used for the element.
    pub sstable_id: SSTableId,
    /// The entry used for the element.
    pub entry: Entry<K, V>,
    /// The iterator index used for the element.
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
            Ordering::Equal => {
                match self
                    .entry
                    .value
                    .sequence_number
                    .cmp(&other.entry.value.sequence_number)
                {
                    Ordering::Equal => self.sstable_id.cmp(&other.sstable_id),
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

/// A struct that represents a stream that merges multiple sorted SSTables into a single sorted stream.
pub struct MergeStream<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The heap used to efficiently find the smallest key across all sources.
    heap: BinaryHeap<MergeElement<K, V>>,
    /// The iterators used for the SSTables.
    iters: Vec<SSTableIterator<K, V>>,
}

impl<K, V> MergeStream<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Creates a new MergeStream from a set of SSTables.
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

        while let Some(peeked) = self.heap.peek() {
            if peeked.entry.key != winner.entry.key {
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
