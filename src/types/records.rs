use crate::SSTableId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ord;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;

pub trait DBKey: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug {}
impl<T> DBKey for T where T: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug
{}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A full key-value pair as stored in an SSTable data block or returned by iterators.
pub struct Entry<K, V> {
    pub key: K,
    pub value: ValueEntry<V>,
}

impl<K: Clone, V> Clone for Entry<K, V> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum LogEntry<K, V> {
    Put(K, Arc<V>),
    Delete(K),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ManifestEntry {
    AddSSTable { level: usize, path: PathBuf },
    RemoveSSTable { level: usize, path: PathBuf },
    NextID(SSTableId),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// The value part of a database entry, including tombstone information.
pub struct ValueEntry<V> {
    pub value: Option<Arc<V>>,
    pub is_tombstone: bool,
}

impl<V> Clone for ValueEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            is_tombstone: self.is_tombstone,
        }
    }
}
