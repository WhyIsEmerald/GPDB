use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ord;
use std::hash::Hash;

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry<V> {
    pub value: V,
    pub is_tombstone: bool,
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
