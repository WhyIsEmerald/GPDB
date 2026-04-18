use gpdb::{DeltaBlockBuilder, Entry, ValueEntry};
use proptest::prelude::*;
use std::sync::Arc;

// Define a simple key-value structure for testing.
// Using String since it's already implemented for DBKey.

proptest! {
    #[test]
    fn delta_encoding_roundtrip(keys in prop::collection::vec(any::<String>(), 1..100)) {
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        sorted_keys.dedup(); // DataBlocks assume sorted, unique keys from MemTable/Compaction.

        let mut builder = DeltaBlockBuilder::new(1024 * 1024); // Large enough to not trigger is_full
        let mut entries = Vec::new();

        for key in &sorted_keys {
            let val = format!("val-{}", key);
            let entry = ValueEntry {
                value: Some(Arc::new(val)),
                is_tombstone: false,
            };
            builder.add(key, &entry);
            entries.push(Entry {
                key: key.clone(),
                value: entry,
            });
        }

        let block = builder.finish();
        let recovered: Vec<_> = block.iter().collect();

        prop_assert_eq!(recovered.len(), entries.len());
        for (idx, rec) in recovered.iter().enumerate() {
            prop_assert_eq!(&rec.key, &entries[idx].key);
            prop_assert_eq!(rec.value.value.as_ref().unwrap().as_str(),
                           entries[idx].value.value.as_ref().unwrap().as_str());
        }
    }

    #[test]
    fn block_boundaries(keys in prop::collection::vec(any::<String>(), 50..200)) {
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        sorted_keys.dedup();

        let target_size = 4096; // 4KB
        let mut builder = DeltaBlockBuilder::new(target_size);
        let mut entries_written = 0;

        for key in &sorted_keys {
            let val = "x".repeat(100); // Constant size
            let entry = ValueEntry {
                value: Some(Arc::new(val)),
                is_tombstone: false,
            };
            builder.add(key, &entry);
            entries_written += 1;

            if builder.is_full() {
                let block = builder.finish();
                let count = block.iter().count();
                prop_assert!(count > 0);
                prop_assert!(count <= entries_written);
                break;
            }
        }
    }
}
