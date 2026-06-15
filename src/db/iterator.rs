//! Merged iterator implementation.
//!
//! Provides a unified, sorted view of entries from multiple sources (MemTables and SSTables),
//! handling MVCC resolution and tombstone filtering.

use crate::db::memtable::SkipMapIterator;
use crate::db::sstable::iterator::SSTableIterator;
use crate::{DBKey, Entry, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// An enum that represents a wrapper for sorted entry sources, abstracting over MemTables and SSTables.
pub enum Source<'a, K, V> {
    /// The source providing entries from an on-disk SSTable.
    SSTable {
        /// The iterator used for the SSTable.
        it: SSTableIterator<K, V>,
        /// The snapshot sequence number used for the SSTable.
        snapshot_seq: u64,
    },
    /// The source providing entries from an in-memory MemTable.
    MemTable {
        /// The iterator used for the MemTable.
        it: SkipMapIterator<'a, K, V>,
        /// The snapshot sequence number used for the MemTable.
        snapshot_seq: u64,
        /// The buffer used for the MemTable.
        buffer: Option<Entry<K, V>>,
    },
}

impl<'a, K, V> Iterator for Source<'a, K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self {
                Source::SSTable { it, .. } => it.next()?,
                Source::MemTable {
                    it,
                    buffer,
                    snapshot_seq,
                    ..
                } => {
                    let first = if let Some(buf) = buffer.take() {
                        Some(buf)
                    } else {
                        it.next().map(|(k, v)| Entry {
                            key: k.clone(),
                            value: v.clone(),
                        })
                    }?;

                    let mut best_version = if first.value.sequence_number < *snapshot_seq {
                        Some(first.clone())
                    } else {
                        None
                    };
                    while let Some((k, v)) = it.next() {
                        if k == first.key {
                            if v.sequence_number < *snapshot_seq {
                                if best_version
                                    .as_ref()
                                    .map_or(true, |bv| v.sequence_number > bv.value.sequence_number)
                                {
                                    best_version = Some(Entry {
                                        key: k.clone(),
                                        value: v.clone(),
                                    });
                                }
                            }
                        } else {
                            *buffer = Some(Entry {
                                key: k.clone(),
                                value: v.clone(),
                            });
                            break;
                        }
                    }

                    let final_entry = best_version.unwrap_or(first);
                    Ok(final_entry)
                }
            };

            let snapshot_seq = match self {
                Source::SSTable { snapshot_seq, .. } => *snapshot_seq,
                Source::MemTable { snapshot_seq, .. } => *snapshot_seq,
            };

            if let Ok(ref e) = entry {
                if e.value.sequence_number < snapshot_seq {
                    return Some(entry);
                }
            } else {
                return Some(entry);
            }
        }
    }
}

/// A struct that represents an internal wrapper for the merge heap, associating an entry with its source.
struct HeapItem<'a, K, V> {
    /// The entry used for comparison.
    entry: Entry<K, V>,
    /// The source used for the entry.
    source: Source<'a, K, V>,
}

impl<'a, K, V> PartialEq for HeapItem<'a, K, V>
where
    K: DBKey,
{
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key
    }
}

impl<'a, K, V> Eq for HeapItem<'a, K, V> where K: DBKey {}

impl<'a, K, V> PartialOrd for HeapItem<'a, K, V>
where
    K: DBKey,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, K, V> Ord for HeapItem<'a, K, V>
where
    K: DBKey,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so we reverse the comparison to get a min-heap for keys.
        other.entry.key.cmp(&self.entry.key)
    }
}

/// A struct that represents an iterator that merges multiple sorted sources into a single sorted stream.
pub struct MergedIterator<'a, K, V> {
    /// The heap used to find the smallest key across all sources.
    heap: BinaryHeap<HeapItem<'a, K, V>>,
}

impl<'a, K, V> MergedIterator<'a, K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Creates a new merged iterator from a collection of sorted sources.
    pub fn new(sources: Vec<Source<'a, K, V>>) -> Self {
        let mut heap = BinaryHeap::new();
        for mut source in sources {
            if let Some(Ok(entry)) = source.next() {
                heap.push(HeapItem { entry, source });
            }
        }
        Self { heap }
    }
}

impl<'a, K, V> Iterator for MergedIterator<'a, K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut item = self.heap.pop()?;
            let current_key = item.entry.key.clone();
            let mut winning_entry = item.entry;

            // MVCC: find the entry with the highest sequence number for the same key
            while let Some(next_item_peek) = self.heap.peek() {
                if next_item_peek.entry.key == current_key {
                    let mut next_item = self.heap.pop().unwrap();
                    if next_item.entry.value.sequence_number > winning_entry.value.sequence_number {
                        winning_entry = next_item.entry;
                    }

                    if let Some(Ok(next_entry)) = next_item.source.next() {
                        self.heap.push(HeapItem {
                            entry: next_entry,
                            source: next_item.source,
                        });
                    }
                } else {
                    break;
                }
            }

            // Push the source of the winning entry back into the heap
            if let Some(Ok(next_entry)) = item.source.next() {
                self.heap.push(HeapItem {
                    entry: next_entry,
                    source: item.source,
                });
            }

            // Tombstone filtering: skip entries where is_tombstone == true
            if winning_entry.value.is_tombstone {
                continue;
            }

            return Some(Ok(winning_entry));
        }
    }
}
