use crate::{DBKey, ValueEntry};
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// A lock-free concurrent MemTable using a SkipList.
/// High throughput for multi-threaded writes without mutex contention.
#[derive(Debug)]
pub struct MemTable<K, V>
where
    K: DBKey,
{
    map: SkipMap<Arc<K>, ValueEntry<V>>,
}

impl<K, V> MemTable<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
        }
    }

    pub fn put(&self, key: Arc<K>, value: Arc<V>) {
        let entry = ValueEntry {
            value: Some(value),
            is_tombstone: false,
        };
        self.map.insert(key, entry);
    }

    pub fn delete(&self, key: Arc<K>) {
        let entry = ValueEntry {
            value: None,
            is_tombstone: true,
        };
        self.map.insert(key, entry);
    }

    pub fn get(&self, key: &Arc<K>) -> Option<Arc<V>> {
        self.map
            .get(key)
            .filter(|entry| !entry.value().is_tombstone)
            .and_then(|entry| entry.value().value.clone())
    }

    pub fn get_entry(&self, key: &Arc<K>) -> Option<ValueEntry<V>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn len(&self) -> usize {
        self.map
            .iter()
            .filter(|entry| !entry.value().is_tombstone)
            .count()
    }

    pub fn clear(&self) {
        self.map.clear();
    }

    /// Returns a lock-free sorted iterator over the MemTable.
    pub fn iter(&self) -> SkipMapIterator<'_, K, V> {
        SkipMapIterator {
            iter: self.map.iter(),
        }
    }
}

pub struct SkipMapIterator<'a, K, V> {
    iter: crossbeam_skiplist::map::Iter<'a, Arc<K>, ValueEntry<V>>,
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
            .map(|entry| (Arc::clone(entry.key()), entry.value().clone()))
    }
}
