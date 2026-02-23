use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ord;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// Entry stores the value for a key in the database together with a tombstone flag.
///
/// `value` contains the actual stored data of generic type `V`. `is_tombstone` is set to
/// true when the entry represents a deletion rather than a live value. Tombstones are
/// persisted so deletions are durable and can be observed during reads, and so that
/// compaction/merge processes can correctly remove or reconcile deleted keys.
pub struct Entry<V> {
    pub value: Option<Arc<V>>,
    pub is_tombstone: bool,
}

impl<V> Clone for Entry<V> {
    fn clone(&self) -> Self {
        Entry {
            value: self.value.clone(),
            is_tombstone: self.is_tombstone,
        }
    }
}

/// Keys must be:
/// - `Eq`: Able to be compared for equality.
/// - `Hash`: Able to be hashed (for use in `HashMap`).
/// - `Ord`: Able to be totally ordered (for sorting in SSTables).
/// - `Clone`: Able to be cloned (for various internal operations).
/// - `Serialize`: Able to be serialized (for writing to disk).
/// - `DeserializeOwned`: Able to be deserialized from disk.
pub trait DBKey: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned {}

impl<T> DBKey for T where T: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned {}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum LogEntry<K, V> {
    Put(K, Arc<V>),
    Delete(K),
}
