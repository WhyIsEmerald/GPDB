use crate::types::{DBKey, Entry};
use std::collections::HashMap;

/// A simple in-memory key-value store, acting as the write-back cache.
///
/// Keys `K` must implement `DBKey` for hashing, ordering, cloning, and serialization.
/// Values `V` must be cloneable for some operations
pub struct MemTable<K, V>
where
    K: DBKey,
    V: Clone,
{
    data: HashMap<K, Entry<V>>,
}

impl<K, V> MemTable<K, V>
where
    K: DBKey,
    V: Clone + Default,
{
    /// Creates a new, empty `MemTable`.
    pub fn new() -> Self {
        MemTable {
            data: HashMap::new(),
        }
    }

    /// Inserts/Updates a key-value pair into the `MemTable`.
    pub fn put(&mut self, key: K, value: V) {
        let entry = Entry {
            value: Some(value),
            is_tombstone: false,
        };
        self.data.insert(key, entry);
    }

    /// Marks a key as deleted by inserting a tombstone.
    pub fn delete(&mut self, key: K) {
        let entry = Entry {
            value: None,
            is_tombstone: true,
        };
        self.data.insert(key, entry);
    }

    /// Retrieves the `Entry` associated with a key, if it exists and is not a tombstone.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.data
            .get(key)
            .filter(|entry| !entry.is_tombstone)
            .and_then(|entry| entry.value.as_ref())
    }

    /// Returns the number of non-tombstone key-value pairs in the `MemTable`.
    pub fn len(&self) -> usize {
        self.data
            .iter()
            .filter(|(_, entry)| !entry.is_tombstone)
            .count()
    }
}
