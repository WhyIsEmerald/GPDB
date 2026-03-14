use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ord;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Data corruption: {0}")]
    Corruption(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Unique identifier for an SSTable.
pub struct SSTableId(pub u64);

impl std::fmt::Display for SSTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Pad to 20 digits for lexicographical alignment in file systems
        write!(f, "{:020}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// The value part of a database entry, including tombstone information.
pub struct ValueEntry<V> {
    pub value: Option<Arc<V>>,
    pub is_tombstone: bool,
}

impl<V> Clone for ValueEntry<V> {
    fn clone(&self) -> Self {
        ValueEntry {
            value: self.value.clone(),
            is_tombstone: self.is_tombstone,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
/// A full key-value pair as stored in an SSTable data block or returned by iterators.
pub struct Entry<K, V> {
    pub key: K,
    pub value: ValueEntry<V>,
}

pub trait DBKey: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned {}

impl<T> DBKey for T where T: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned {}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum LogEntry<K, V> {
    Put(K, Arc<V>),
    Delete(K),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum ManifestEntry {
    AddSSTable { level: usize, path: PathBuf },
    RemoveSSTable { level: usize, path: PathBuf },
    NextID(SSTableId),
}
