use crate::{DBKey, Entry, ValueEntry};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

/// A batch of write operations (Put/Delete) that are applied together atomically.
#[derive(Debug, Default, Clone)]
pub struct WriteBatch<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    pub(crate) entries: Vec<Entry<K, V>>,
}

impl<K, V> WriteBatch<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new empty write batch.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Adds a put operation to the batch.
    pub fn put(&mut self, key: K, value: V) {
        self.entries.push(Entry {
            key,
            value: ValueEntry {
                value: Some(Arc::new(value)),
                is_tombstone: false,
            },
        });
    }

    /// Adds a delete operation to the batch.
    pub fn delete(&mut self, key: K) {
        self.entries.push(Entry {
            key,
            value: ValueEntry {
                value: None,
                is_tombstone: true,
            },
        });
    }

    /// Returns the number of operations in the batch.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clears the batch for reuse.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
