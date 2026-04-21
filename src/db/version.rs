use crate::SSTable;
use crate::types::records::DBKey;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// An immutable point-in-time view of the database's SSTables.
#[derive(Debug)]
pub struct Version<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub levels: Vec<Vec<SSTable<K, V>>>,
}

impl<K, V> Version<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(levels: Vec<Vec<SSTable<K, V>>>) -> Self {
        Self { levels }
    }
}
