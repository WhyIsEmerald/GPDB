//! Core record types for GPDB.
//!
//! Defines the fundamental data structures for keys, values, log entries, and manifest records.

use crate::SSTableId;
use crate::types::sizable::Sizable;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ord;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;

/// A trait that defines the requirements for a type to be used as a database key.
pub trait DBKey:
    Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug + Sizable
{
}
impl<T> DBKey for T where
    T: Eq + Hash + Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug + Sizable
{
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A struct that represents a key-value pair used in SSTables and iterators.
pub struct Entry<K, V> {
    /// The key used for the entry.
    pub key: Arc<K>,
    /// The value entry used for the entry.
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
/// An enum that represents the type of operation stored in the write-ahead log.
pub enum LogOperation<K, V> {
    /// The put operation used for insertion or update.
    Put(Arc<K>, Arc<V>),
    /// The delete operation used for deletion.
    Delete(Arc<K>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A struct that represents a single record in the write-ahead log.
pub struct LogEntry<K, V> {
    /// The sequence number used for the operation.
    pub sequence_number: u64,
    /// The operation used for the record.
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
/// An enum that represents a record in the database manifest tracking state changes.
pub enum ManifestEntry {
    /// The add sstable operation used for adding an SSTable.
    AddSSTable { level: usize, path: PathBuf },
    /// The remove sstable operation used for removing an SSTable.
    RemoveSSTable { level: usize, path: PathBuf },
    /// The next id operation used for the next available SSTable ID.
    NextID(SSTableId),
    /// The flush wal operation used for a flushed WAL file.
    FlushWal { wal_id: u64 },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A struct that represents the value and metadata of a database entry.
pub struct ValueEntry<V> {
    /// The value used for the entry.
    pub value: Option<Arc<V>>,
    /// The tombstone flag used for the entry.
    pub is_tombstone: bool,
    /// The sequence number used for the value.
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
