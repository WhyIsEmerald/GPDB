use crate::SSTable;
use crate::db::memtable::MemTable;
use crate::types::records::DBKey;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// An immutable point-in-time view of the database's SSTables and Immutable MemTables.
#[derive(Debug)]
pub struct VersionState<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub levels: Vec<Vec<SSTable<K, V>>>,
    pub immutables: Vec<Arc<MemTable<K, V>>>,
}

impl<K, V> VersionState<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(levels: Vec<Vec<SSTable<K, V>>>, immutables: Vec<Arc<MemTable<K, V>>>) -> Self {
        Self { levels, immutables }
    }
}
