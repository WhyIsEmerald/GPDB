use crate::{DBKey, ValueEntry};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::sync::Arc;

/// A single shard of the MemTable, protected by its own RwLock.
#[derive(Debug)]
pub struct MemTableShard<K, V>
where
    K: DBKey,
{
    data: RwLock<ShardData<K, V>>,
}

#[derive(Debug)]
struct ShardData<K, V> {
    hash_map: HashMap<K, ValueEntry<V>>,
    b_tree_map: BTreeMap<K, ValueEntry<V>>,
}

impl<K, V> MemTableShard<K, V>
where
    K: DBKey,
{
    pub fn new() -> Self {
        Self {
            data: RwLock::new(ShardData {
                hash_map: HashMap::new(),
                b_tree_map: BTreeMap::new(),
            }),
        }
    }

    pub fn put(&self, key: K, value: Arc<V>) {
        let entry = ValueEntry {
            value: Some(value),
            is_tombstone: false,
        };
        let mut data = self.data.write();
        data.hash_map.insert(key.clone(), entry.clone());
        data.b_tree_map.insert(key, entry);
    }

    pub fn delete(&self, key: K) {
        let entry = ValueEntry {
            value: None,
            is_tombstone: true,
        };
        let mut data = self.data.write();
        data.hash_map.insert(key.clone(), entry.clone());
        data.b_tree_map.insert(key, entry);
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let data = self.data.read();
        data.hash_map
            .get(key)
            .filter(|entry| !entry.is_tombstone)
            .and_then(|entry| entry.value.clone())
    }

    pub fn get_entry(&self, key: &K) -> Option<ValueEntry<V>> {
        let data = self.data.read();
        data.hash_map.get(key).cloned()
    }

    pub fn clear(&self) {
        let mut data = self.data.write();
        data.hash_map.clear();
        data.b_tree_map.clear();
    }
}

/// ShardedMemTable distributes keys across multiple MemTableShards to reduce lock contention.
#[derive(Debug)]
pub struct MemTable<K, V>
where
    K: DBKey,
{
    shards: Vec<MemTableShard<K, V>>,
    num_shards: usize,
}

impl<K, V> MemTable<K, V>
where
    K: DBKey,
{
    pub fn new() -> Self {
        let num_shards = 16;
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(MemTableShard::new());
        }
        Self { shards, num_shards }
    }

    fn get_shard_idx(&self, key: &K) -> usize {
        use std::collections::hash_map::DefaultHasher;
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) % self.num_shards
    }

    pub fn put(&self, key: K, value: Arc<V>) {
        let idx = self.get_shard_idx(&key);
        self.shards[idx].put(key, value);
    }

    pub fn delete(&self, key: K) {
        let idx = self.get_shard_idx(&key);
        self.shards[idx].delete(key);
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let idx = self.get_shard_idx(key);
        self.shards[idx].get(key)
    }

    pub fn get_entry(&self, key: &K) -> Option<ValueEntry<V>> {
        let idx = self.get_shard_idx(key);
        self.shards[idx].get_entry(key)
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.data
                    .read()
                    .hash_map
                    .values()
                    .filter(|v| !v.is_tombstone)
                    .count()
            })
            .sum()
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    /// Returns a merged, sorted iterator over all shards.
    pub fn iter(&self) -> MergedShardIterator<K, ValueEntry<V>> {
        // Collect into a single BTreeMap during flush.
        // This holds all shard locks simultaneously.
        let mut merged = BTreeMap::new();
        for shard in &self.shards {
            let guard = shard.data.read();
            for (k, v) in &guard.b_tree_map {
                merged.insert(k.clone(), v.clone());
            }
        }
        MergedShardIterator {
            data: merged.into_iter(),
        }
    }
}

pub struct MergedShardIterator<K, V> {
    data: std::collections::btree_map::IntoIter<K, V>,
}

impl<K, V> Iterator for MergedShardIterator<K, V> {
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.data.next()
    }
}
