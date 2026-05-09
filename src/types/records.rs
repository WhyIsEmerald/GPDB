use crate::SSTableId;
use crate::types::sizable::Sizable;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ord;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;

pub trait DBKey:
    Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug + Sizable
{
}
impl<T> DBKey for T where
    T: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug + Sizable
{
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A full key-value pair as stored in an SSTable data block or returned by iterators.
pub struct Entry<K, V> {
    pub key: Arc<K>,
    pub value: ValueEntry<V>,
}

impl<K: Clone, V> Clone for Entry<K, V> {
    fn clone(&self) -> Self {
        Self {
            key: Arc::clone(&self.key),
            value: self.value.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum LogOperation<K, V> {
    Put(Arc<K>, Arc<V>),
    Delete(Arc<K>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct LogEntry<K, V> {
    pub sequence_number: u64,
    pub operation: LogOperation<K, V>,
}

impl<K: Clone, V> Clone for LogEntry<K, V> {
    fn clone(&self) -> Self {
        Self {
            sequence_number: self.sequence_number,
            operation: match &self.operation {
                LogOperation::Put(k, v) => LogOperation::Put(Arc::clone(k), Arc::clone(v)),
                LogOperation::Delete(k) => LogOperation::Delete(Arc::clone(k)),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ManifestEntry {
    AddSSTable { level: usize, path: PathBuf },
    RemoveSSTable { level: usize, path: PathBuf },
    NextID(SSTableId),
    FlushWal { wal_id: u64 },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// The value part of a database entry, including tombstone information.
pub struct ValueEntry<V> {
    pub value: Option<Arc<V>>,
    pub is_tombstone: bool,
    pub sequence_number: u64,
}

impl<V> Clone for ValueEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.as_ref().map(Arc::clone),
            is_tombstone: self.is_tombstone,
            sequence_number: self.sequence_number,
        }
    }
}
