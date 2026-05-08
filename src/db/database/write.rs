use crate::db::database::DB;
use crate::{DBKey, LogEntry, LogOperation, Result, WriteBatch};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
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
            let key_size = calculate_size(&*entry.key);
            let val_size = entry
                .value
                .value
                .as_ref()
                .map_or(0, |v| calculate_size(&**v));
            total_batch_size += key_size + val_size;
            let seq = self.sequence_number.fetch_add(1, Ordering::SeqCst);
            let log_entry = if entry.value.is_tombstone {
                LogEntry {
                    sequence_number: seq,
                    operation: LogOperation::Delete(entry.key),
                }
            } else {
                LogEntry {
                    sequence_number: seq,
                    operation: LogOperation::Put(
                        entry.key,
                        entry.value.value.expect("Value missing"),
                    ),
                }
            };
            log_entries.push(log_entry);
        }

        let entries_arc = Arc::new(log_entries);

        // Group Commit via WalManager (Zero-copy send)
        self.wal.submit(Arc::clone(&entries_arc))?;

        let memtable = self.memtable.load();
        for entry in entries_arc.iter() {
            match &entry.operation {
                LogOperation::Put(k, v) => {
                    memtable.put(Arc::clone(k), Arc::clone(v), entry.sequence_number)
                }
                LogOperation::Delete(k) => memtable.delete(Arc::clone(k), entry.sequence_number),
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

fn calculate_size<T: 'static>(val: &T) -> usize {
    let any_val = val as &dyn std::any::Any;
    if let Some(s) = any_val.downcast_ref::<String>() {
        return s.len() + std::mem::size_of::<String>();
    }
    if let Some(v) = any_val.downcast_ref::<Vec<u8>>() {
        return v.len() + std::mem::size_of::<Vec<u8>>();
    }
    std::mem::size_of_val(val)
}
