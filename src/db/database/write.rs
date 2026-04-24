use crate::db::database::DB;
use crate::{DBKey, LogEntry, Result, WriteBatch};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::atomic::Ordering;

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub fn put(&self, key: K, value: V) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch(batch)
    }

    pub fn delete(&self, key: K) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch(batch)
    }

    pub fn write_batch(&self, batch: WriteBatch<K, V>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.handle_compaction_results()?;

        let mut log_entries = Vec::with_capacity(batch.entries.len());
        let mut total_batch_size = 0;
        for entry in batch.entries {
            let key_size = std::mem::size_of_val(&entry.key);
            let val_size = entry
                .value
                .value
                .as_ref()
                .map_or(0, |v| std::mem::size_of_val(&**v));
            total_batch_size += key_size + val_size;
            let log_entry = if entry.value.is_tombstone {
                LogEntry::Delete(entry.key.clone())
            } else {
                LogEntry::Put(
                    entry.key.clone(),
                    entry.value.value.clone().expect("Value missing"),
                )
            };
            log_entries.push(log_entry);
        }

        {
            self.wal.submit(log_entries.clone())?;
        }

        let memtable = self.memtable.load();
        for entry in log_entries {
            match entry {
                LogEntry::Put(k, v) => memtable.put(k, v),
                LogEntry::Delete(k) => memtable.delete(k),
            }
        }

        if self
            .config
            .memtable_size
            .fetch_add(total_batch_size, Ordering::Relaxed)
            + total_batch_size
            >= self.config.max_memtable_size
        {
            self.switch_memtable()?;
        }
        Ok(())
    }
}
