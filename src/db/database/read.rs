use crate::types::records::ValueEntry;
use crate::{DB, DBKey, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ReadResult<V> {
    pub value: Option<Arc<V>>,
    pub sstables_touched: usize,
}

impl<K, V> DB<K, V>
where
    K: DBKey + Send + Sync + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    pub fn get(&self, key: &K) -> Result<ReadResult<V>> {
        let key_arc = Arc::new(key.clone());
        if let Some(entry) = self.memtable.load().get_entry(&key_arc) {
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

        let version = self.version.load();
        for imm in version.immutables.iter().rev() {
            if let Some(entry) = imm.memtable.get_entry(&key_arc) {
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

        let mut best_entry: Option<ValueEntry<V>> = None;
        let mut touched = 0;
        for level in &version.levels {
            for sstable in level.iter().rev() {
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

        Ok(ReadResult {
            value: best_entry.and_then(|e| if e.is_tombstone { None } else { e.value }),
            sstables_touched: touched,
        })
    }
}
