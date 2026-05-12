use gpdb::{DBKey, Entry, Error, Result, SSTable, SSTableId};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_tombstone_eviction_at_max_level() -> Result<()> {
    let tmp_dir = tempdir().map_err(|e| Error::Io(Arc::new(e)))?;
    let path = tmp_dir.path();

    let key = Arc::new("test_key".to_string());
    let val = Arc::new("test_val".to_string());

    // 1. Create an SSTable with the value
    let entries = vec![Ok(Entry {
        key: Arc::clone(&key),
        value: gpdb::ValueEntry {
            value: Some(Arc::clone(&val)),
            sequence_number: 1,
            is_tombstone: false,
        },
    })];

    let sst_path1 = path.join("l0_1.sst");
    let sst1 = SSTable::write_from_iter(&sst_path1, entries.into_iter(), SSTableId(1), 0, None)?;

    // 2. Create an SSTable with the tombstone
    let entries_tombstone = vec![Ok(Entry {
        key: Arc::clone(&key),
        value: gpdb::ValueEntry {
            value: None,
            sequence_number: 2,
            is_tombstone: true,
        },
    })];

    let sst_path2 = path.join("l0_2.sst");
    let sst2 = SSTable::write_from_iter(
        &sst_path2,
        entries_tombstone.into_iter(),
        SSTableId(2),
        0,
        None,
    )?;

    // 3. Compact them together at MAX_LEVEL
    let sstables = vec![sst1, sst2];
    let output_path = path.join("lmax.sst");
    let lmax = gpdb::db::database::MAX_LEVEL;

    let compact_result =
        gpdb::db::compaction::Compactor::compact(&sstables, &output_path, SSTableId(3), lmax, None);

    // 4. Verify the tombstone is gone.
    // If the result is "Empty SSTable", it means the tombstone was successfully evicted
    // and no other data remains, which is the expected outcome here.
    match compact_result {
        Ok(compacted_sst) => {
            let result = compacted_sst.get(&key)?;
            assert!(result.is_none(), "Key should be evicted at Lmax");
            assert_eq!(
                compacted_sst.len(),
                0,
                "SSTable at Lmax should be empty after tombstone eviction"
            );
        }
        Err(Error::Corruption(e)) if e == "Empty SSTable" => {
            // Success: The tombstone was evicted and the SSTable became empty.
        }
        Err(e) => panic!("Unexpected error during compaction: {:?}", e),
    }

    Ok(())
}
