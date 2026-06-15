//! MemTable implementation.
//!
//! Provides an in-memory sorted storage using a lock-free SkipList for high-concurrency writes.

use crate::{DBKey, ValueEntry};
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// A struct that represents a concurrent, lock-free in-memory table.
#[derive(Debug)]
pub struct MemTable<K, V>
where
    K: DBKey,
{
    /// The map used for storing key-sequence pairs and their values.
    map: SkipMap<(Arc<K>, u64), ValueEntry<V>>,
}

impl<K, V> Default for MemTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MemTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Creates a new, empty MemTable.
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
        }
    }

    /// Inserts a key-value pair into the MemTable.
    pub fn put(&self, key: Arc<K>, value: Arc<V>, sequence_number: u64) {
        let entry = ValueEntry {
            value: Some(value),
            is_tombstone: false,
            sequence_number,
        };
        self.map.insert((key, sequence_number), entry);
    }

    /// Marks a key as deleted in the MemTable by inserting a tombstone.
    pub fn delete(&self, key: Arc<K>, sequence_number: u64) {
        let entry = ValueEntry {
            value: None,
            is_tombstone: true,
            sequence_number,
        };
        self.map.insert((key, sequence_number), entry);
    }

    /// Retrieves a value for a given key.
    pub fn get(&self, key: &Arc<K>, sequence_number: u64) -> Option<Arc<V>> {
        self.get_entry(key, sequence_number)
            .filter(|entry| !entry.is_tombstone)
            .and_then(|entry| entry.value.clone())
    }

    /// Retrieves the most recent entry for a key up to a specific sequence number.
    pub fn get_entry(&self, key: &Arc<K>, sequence_number: u64) -> Option<ValueEntry<V>> {
        self.map
            .range((key.clone(), 0)..(key.clone(), sequence_number))
            .next_back()
            .map(|entry| entry.value().clone())
    }

    /// Returns the number of non-tombstone entries in the MemTable.
    pub fn len(&self) -> usize {
        self.map
            .iter()
            .filter(|entry| !entry.value().is_tombstone)
            .count()
    }

    /// Checks if the MemTable is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all entries from the MemTable.
    pub fn clear(&self) {
        self.map.clear();
    }

    /// Returns a sorted iterator over all entries in the MemTable.
    pub fn iter(&self) -> SkipMapIterator<'_, K, V> {
        SkipMapIterator {
            iter: self.map.iter(),
        }
    }
}

/// A sorted iterator over a MemTable.
pub struct SkipMapIterator<'a, K, V> {
    /// The underlying skip list iterator.
    iter: crossbeam_skiplist::map::Iter<'a, (Arc<K>, u64), ValueEntry<V>>,
}

impl<'a, K, V> Iterator for SkipMapIterator<'a, K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    type Item = (Arc<K>, ValueEntry<V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|entry| (Arc::clone(&entry.key().0), entry.value().clone()))
    }
}
