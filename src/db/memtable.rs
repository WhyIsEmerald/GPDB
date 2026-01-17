use crate::types::{DBKey, Entry};
use std::collections::{BTreeMap, HashMap};

/// A simple in-memory key-value store, acting as the write-back cache.
///
/// Keys `K` must implement `DBKey` for hashing, ordering, cloning, and serialization.
/// Values `V` must be cloneable for some operations
pub struct MemTable<K, V>
where
    K: DBKey,
    V: Clone,
{
    b_tree_map: BTreeMap<K, Entry<V>>,
    hash_map: HashMap<K, Entry<V>>,
}

impl<K, V> MemTable<K, V>
where
    K: DBKey,
    V: Clone,
{
    /// Creates a new, empty `MemTable`.
    pub fn new() -> Self {
        MemTable {
            hash_map: HashMap::new(),
            b_tree_map: BTreeMap::new(),
        }
    }

    /// Inserts/Updates a key-value pair into the `MemTable`.
    pub fn put(&mut self, key: K, value: V) -> Option<Entry<V>> {
        let entry = Entry {
            value: Some(value),
            is_tombstone: false,
        };
        self.b_tree_map.insert(key.clone(), entry.clone());
        self.hash_map.insert(key, entry)
    }

    /// Marks a key as deleted by inserting a tombstone.
    pub fn delete(&mut self, key: K) -> Option<Entry<V>> {
        let entry = Entry {
            value: None,
            is_tombstone: true,
        };
        self.b_tree_map.insert(key.clone(), entry.clone());
        self.hash_map.insert(key, entry)
    }

    /// Retrieves the `Entry` associated with a key, if it exists and is not a tombstone.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.hash_map
            .get(key)
            .filter(|entry| !entry.is_tombstone)
            .and_then(|entry| entry.value.as_ref())
    }

    /// Returns the number of non-tombstone key-value pairs in the `MemTable`.
    pub fn len(&self) -> usize {
        self.hash_map
            .iter()
            .filter(|(_, entry)| !entry.is_tombstone)
            .count()
    }
}
