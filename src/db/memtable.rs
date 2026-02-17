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
        // deleted values are still iterable since this functionality is required for the sstable
        self.b_tree_map.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Entry;
    use serde::{Deserialize, Serialize};
    use std::cmp::{Ordering, PartialOrd};
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;

    #[test]
    fn new_memtable() {
        let memtable: MemTable<String, String> = MemTable::new();
        assert_eq!(memtable.len(), 0);
        assert!(memtable.b_tree_map.is_empty());
        assert!(memtable.hash_map.is_empty());
    }

    #[test]
    fn put_get() {
        let mut memtable: MemTable<String, String> = MemTable::new();

        let key = "key".to_string();
        let arc_value = Arc::new("value".to_string());
        memtable.put(key.clone(), arc_value.clone());
        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(&key), Some(Arc::new("value".to_string())));
    }

    #[test]
    fn put_overwrite() {
        let mut memtable: MemTable<String, String> = MemTable::new();

        let key = "key".to_string();
        let arc_value1 = Arc::new("value1".to_string());
        let arc_value2 = Arc::new("value2".to_string());
        memtable.put(key.clone(), arc_value1.clone());
        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(&key), Some(Arc::new("value1".to_string())));

        memtable.put(key.clone(), arc_value2.clone());
        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(&key), Some(Arc::new("value2".to_string())));
    }

    #[test]
    fn delete_value() {
        let mut memtable: MemTable<String, String> = MemTable::new();

        let key = "key".to_string();
        let arc_value = Arc::new("value".to_string());
        memtable.put(key.clone(), arc_value.clone());
        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(&key), Some(Arc::new("value".to_string())));

        memtable.delete(key.clone());
        assert_eq!(memtable.len(), 0);
        assert!(memtable.get(&key).is_none());
        assert_eq!(memtable.hash_map.len(), 1) // value still exists as tomstone
    }

    #[test]
    fn clear_all() {
        let mut memtable: MemTable<String, String> = MemTable::new();

        let key1 = "key1".to_string();
        let arc_value1 = Arc::new("value1".to_string());
        memtable.put(key1.clone(), arc_value1.clone());

        let key2 = "key2".to_string();
        let arc_value2 = Arc::new("value2".to_string());
        memtable.put(key2.clone(), arc_value2.clone());

        memtable.clear();
        assert_eq!(memtable.len(), 0);
        assert!(memtable.get(&key1).is_none());
        assert!(memtable.get(&key2).is_none());
        assert!(memtable.hash_map.is_empty());
        assert!(memtable.b_tree_map.is_empty());
    }

    #[test]
    fn iter() {
        let mut memtable: MemTable<String, String> = MemTable::new();

        let key1 = "key1".to_string();
        let arc_value1 = Arc::new("value1".to_string());
        memtable.put(key1.clone(), arc_value1.clone());

        let key2 = "key2".to_string();
        let arc_value2 = Arc::new("value2".to_string());
        memtable.put(key2.clone(), arc_value2.clone());

        let mut iter = memtable.iter();

        let (k, entry) = iter.next().expect("Iterator should have at least 1 item");
        assert_eq!(k, &key1);
        assert!(!entry.is_tombstone);
        let v1 = entry.value.as_ref().unwrap();
        assert_eq!(v1.as_str(), "value1");

        let (k, entry) = iter.next().expect("Iterator should have at least 2 items");
        assert_eq!(k, &key2);
        assert!(!entry.is_tombstone);
        let v2 = entry.value.as_ref().unwrap();
        assert_eq!(v2.as_str(), "value2");
        assert!(iter.next().is_none());
    }

    #[test]
    fn iter_tombstone() {
        let mut memtable: MemTable<String, String> = MemTable::new();

        let key1 = "key1".to_string();
        let arc_value1 = Arc::new("value1".to_string());
        memtable.put(key1.clone(), arc_value1.clone());

        let key2 = "key2".to_string();
        let arc_value2 = Arc::new("value2".to_string());
        memtable.put(key2.clone(), arc_value2.clone());

        memtable.delete(key1.clone());

        let mut iter = memtable.iter();

        // deleted values are still iterable since this functionality is required for the sstable
        let (k, entry) = iter.next().expect("Iterator should have at least 2 item");
        assert_eq!(k, &key1);
        assert!(entry.is_tombstone);
        assert!(entry.value.is_none());

        let (k, entry) = iter.next().expect("Iterator should have at least 2 item");
        assert_eq!(k, &key2);
        assert!(!entry.is_tombstone);
        assert_eq!(entry.value.as_ref().unwrap(), &arc_value2);

        assert!(iter.next().is_none());
    }
}
