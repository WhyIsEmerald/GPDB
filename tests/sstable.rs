use gpdb::{Entry, FilterVariant, MemTable, SSTable, SSTableId, ValueEntry};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

fn setup() -> (TempDir, PathBuf) {
    let tmp_dir = TempDir::new().expect("Failed to create temporary directory");
    let sstable_path = tmp_dir.path().join("L0-00001.sst");
    (tmp_dir, sstable_path)
}

#[test]
fn tiered_xor_filters() {
    let (tmp_dir, _) = setup();

    let l0_path = tmp_dir.path().join("L0.sst");
    let memtable = MemTable::new();
    memtable.put(Arc::new("k1".to_string()), Arc::new("v1".to_string()));
    let sst_l0 = SSTable::write_from_memtable(&l0_path, &memtable, SSTableId(1), None).unwrap();
    assert!(matches!(sst_l0.filter(), FilterVariant::Xor8(_)));
    assert!(sst_l0.get(&Arc::new("k1".to_string())).unwrap().is_some());

    let l1_path = tmp_dir.path().join("L1.sst");
    let entries = vec![Ok(Entry {
        key: Arc::new("k2".to_string()),
        value: ValueEntry {
            value: Some(Arc::new("v2".to_string())),
            is_tombstone: false,
        },
    })];
    let sst_l1 =
        SSTable::write_from_iter(&l1_path, entries.into_iter(), SSTableId(2), 1, None).unwrap();
    assert!(matches!(sst_l1.filter(), FilterVariant::Xor16(_)));
    assert!(sst_l1.get(&Arc::new("k2".to_string())).unwrap().is_some());
}

#[test]
fn test_delta_encoding_correctness() {
    let (_tmp_dir, sstable_path) = setup();
    let memtable: MemTable<String, String> = MemTable::new();

    memtable.put(
        Arc::new("user_id_00001".to_string()),
        Arc::new("val1".to_string()),
    );
    memtable.put(
        Arc::new("user_id_00002".to_string()),
        Arc::new("val2".to_string()),
    );
    memtable.put(
        Arc::new("user_id_00003".to_string()),
        Arc::new("val3".to_string()),
    );

    let sst = SSTable::write_from_memtable(&sstable_path, &memtable, SSTableId(1), None).unwrap();
    assert_eq!(
        sst.get(&"user_id_00001".to_string())
            .unwrap()
            .unwrap()
            .value
            .unwrap()
            .as_str(),
        "val1"
    );
    assert_eq!(
        sst.get(&"user_id_00002".to_string())
            .unwrap()
            .unwrap()
            .value
            .unwrap()
            .as_str(),
        "val2"
    );
    assert_eq!(
        sst.get(&"user_id_00003".to_string())
            .unwrap()
            .unwrap()
            .value
            .unwrap()
            .as_str(),
        "val3"
    );

    let entries: Vec<_> = sst.iter().unwrap().map(|r| r.unwrap().key).collect();
    assert_eq!(
        entries,
        vec![
            Arc::new("user_id_00001".to_string()),
            Arc::new("user_id_00002".to_string()),
            Arc::new("user_id_00003".to_string()),
        ]
    );
}
