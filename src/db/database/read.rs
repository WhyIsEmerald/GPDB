use crate::{DB, DBKey, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>> {
        if let Some(entry) = self.memtable.load().get_entry(key) {
            if entry.is_tombstone {
                return Ok(None);
            }
            return Ok(entry.value);
        }

        let version = self.version.load();
        for imm in version.immutables.iter().rev() {
            if let Some(entry) = imm.get_entry(key) {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(entry.value);
            }
        }

        for level in &version.levels {
            for sstable in level.iter().rev() {
                if let Some(val_entry) = sstable.get(key)? {
                    if val_entry.is_tombstone {
                        return Ok(None);
                    }
                    return Ok(val_entry.value);
                }
            }
        }
        Ok(None)
    }
}
