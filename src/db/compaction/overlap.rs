use crate::db::compaction::Compactor;
use crate::{DBKey, SSTable};
use serde::{Serialize, de::DeserializeOwned};

impl Compactor {
    /// Finds all SSTables in `target_level` that overlap with the `candidate`.
    pub fn find_overlapping_sstables<K, V>(
        candidate: &SSTable<K, V>,
        target_level: &[SSTable<K, V>],
    ) -> Vec<usize>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let mut overlapping = Vec::new();
        for (idx, sst) in target_level.iter().enumerate() {
            if candidate.overlaps(sst) {
                overlapping.push(idx);
            }
        }
        overlapping
    }

    /// Finds all SSTables in `target_level` that overlap with the range defined by a set of `sstables`.
    pub fn find_range_overlapping_sstables<K, V>(
        sstables: &[SSTable<K, V>],
        target_level: &[SSTable<K, V>],
    ) -> Vec<usize>
    where
        K: DBKey + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if sstables.is_empty() {
            return Vec::new();
        }

        let min = sstables.iter().map(|s| s.min_key()).min().unwrap();
        let max = sstables.iter().map(|s| s.max_key()).max().unwrap();

        let mut overlapping = Vec::new();
        for (idx, sst) in target_level.iter().enumerate() {
            if sst.overlaps_range(min, max) {
                overlapping.push(idx);
            }
        }
        overlapping
    }
}
