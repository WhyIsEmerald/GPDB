use crate::db::sstable::SSTableIterator;
use crate::{DBKey, Entry, Result, SSTable, SSTableId};
use serde::{Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

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
