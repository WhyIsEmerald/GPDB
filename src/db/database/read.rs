//! Read path implementation for the database.

use crate::types::records::ValueEntry;
use crate::{DB, DBKey, Result, types::sizable::Sizable};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// A struct that represents the result of a read operation.
#[derive(Debug, Serialize)]
pub struct ReadResult<V> {
    /// The value used for the result, or None if not found or deleted.
    pub value: Option<Arc<V>>,
    /// The number of SSTables used for the read operation.
    pub sstables_touched: usize,
}

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug + Sizable,
{
    /// Retrieves a value for the given key.
    pub fn get(
        &self,
        key: &K,
        snapshot: Option<&crate::db::database::Snapshot<K, V>>,
    ) -> Result<ReadResult<V>> {
        let snapshot_seq = snapshot.map(|s| s.seq).unwrap_or_else(|| {
            self.sequence_number
                .load(std::sync::atomic::Ordering::SeqCst)
        });

        let key_arc = Arc::new(key.clone());
        if let Some(entry) = self.memtable.load().get_entry(&key_arc, snapshot_seq) {
            if entry.sequence_number <= snapshot_seq {
                if entry.is_tombstone {
                    return Ok(ReadResult {
                        value: None,
                        sstables_touched: 0,
                    });
                }
                return Ok(ReadResult {
                    value: entry.value,
                    sstables_touched: 0,
                });
            }
        }

        let version = self.version.load();
        for imm in version.immutables.iter().rev() {
            if let Some(entry) = imm.memtable.get_entry(&key_arc, snapshot_seq) {
                if entry.sequence_number <= snapshot_seq {
                    if entry.is_tombstone {
                        return Ok(ReadResult {
                            value: None,
                            sstables_touched: 0,
                        });
                    }
                    return Ok(ReadResult {
                        value: entry.value,
                        sstables_touched: 0,
                    });
                }
            }
        }

        let mut best_entry: Option<ValueEntry<V>> = None;
        let mut touched = 0;
        for level in &version.levels {
            for sstable in level.iter().rev() {
                if sstable.min_seq > snapshot_seq {
                    continue;
                }

                if let Some(ref best) = best_entry {
                    if sstable.max_seq < best.sequence_number {
                        return Ok(ReadResult {
                            value: if best.is_tombstone {
                                None
                            } else {
                                best.value.clone()
                            },
                            sstables_touched: touched,
                        });
                    }
                }

                touched += 1;
                if let Some(val_entry) = sstable.get(key)? {
                    if val_entry.sequence_number <= snapshot_seq {
                        if let Some(ref best) = best_entry {
                            if val_entry.sequence_number > best.sequence_number {
                                best_entry = Some(val_entry);
                            }
                        } else {
                            best_entry = Some(val_entry);
                        }
                    }
                }
            }
        }

        Ok(ReadResult {
            value: best_entry.and_then(|e| if e.is_tombstone { None } else { e.value }),
            sstables_touched: touched,
        })
    }
}
