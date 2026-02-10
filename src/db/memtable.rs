use crate::types::{DBKey, Entry};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A simple in-memory key-value store, acting as the write-back cache.
///
/// Keys `K` must implement `DBKey` for hashing, ordering, cloning, and serialization.
/// Values `V` are stored behind an Arc to avoid expensive clones.
pub struct MemTable<K, V>
where
    K: DBKey,
{
    // For fast, sorted iteration when flushing to SSTable
    b_tree_map: BTreeMap<K, Entry<V>>,
    // For fast O(1) key lookups
    hash_map: HashMap<K, Entry<V>>,
}

impl<K, V> MemTable<K, V>
where
    K: DBKey,
{
    /// Creates a new, empty `MemTable`.
    pub fn new() -> Self {
        MemTable {
            hash_map: HashMap::new(),
            b_tree_map: BTreeMap::new(),
        }
    }

    /// Inserts/Updates a key-value pair. The value is an Arc to avoid expensive clones.
    pub fn put(&mut self, key: K, value: Arc<V>) -> Option<Entry<V>> {
        let entry = Entry {
            value: Some(value),
            is_tombstone: false,
        };
        self.hash_map.insert(key.clone(), entry.clone());
        self.b_tree_map.insert(key, entry)
    }

    /// Marks a key as deleted by inserting a tombstone.
    pub fn delete(&mut self, key: K) -> Option<Entry<V>> {
        let entry = Entry {
            value: None,
            is_tombstone: true,
        };
        self.hash_map.insert(key.clone(), entry.clone());
        self.b_tree_map.insert(key, entry)
    }

    /// Retrieves a shared pointer (`Arc`) to the value associated with a key.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.hash_map
            .get(key)
            .filter(|entry| !entry.is_tombstone)
            .and_then(|entry| entry.value.clone())
    }

    /// Returns the number of non-tombstone key-value pairs in the `MemTable`.
    pub fn len(&self) -> usize {
        self.hash_map
            .iter()
            .filter(|(_, entry)| !entry.is_tombstone)
            .count()
    }

    /// Clears the `MemTable` by removing all key-value pairs.
    pub fn clear(&mut self) {
        self.hash_map.clear();
        self.b_tree_map.clear();
    }

    /// Returns an iterator over the sorted key-value entries in the MemTable.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, K, Entry<V>> {
        self.b_tree_map.iter()
    }
}
