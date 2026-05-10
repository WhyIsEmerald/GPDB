use gpdb::{DB, Snapshot};
use std::path::PathBuf;
use tempfile::TempDir;

fn setup() -> (TempDir, PathBuf) {
    let tmp_dir = TempDir::new().expect("Failed to create temporary directory");
    let path = tmp_dir.path().to_path_buf();
    (tmp_dir, path)
}

#[test]
fn test_merged_iterator_sorted_order() {
    let (_tmp_dir, path) = setup();
    let db = DB::open(&path, 10 * 1024 * 1024).unwrap();

    let keys = vec!["c", "a", "b", "e", "d"];
    for k in &keys {
        db.put(k.to_string(), "val".to_string()).unwrap();
    }

    let snapshot = db.snapshot();
    let mut iter = db.iter(&snapshot).unwrap();
    let mut results = Vec::new();
    while let Some(Ok(entry)) = iter.next() {
        results.push(entry.key.to_string());
    }

    assert_eq!(results, vec!["a", "b", "c", "d", "e"]);
}

#[test]
fn test_merged_iterator_mvcc_resolution() {
    let (_tmp_dir, path) = setup();
    let db = DB::open(&path, 10 * 1024 * 1024).unwrap();

    // Write V1
    db.put("key1".to_string(), "v1".to_string()).unwrap();
    // Write V2
    db.put("key1".to_string(), "v2".to_string()).unwrap();

    let snapshot = db.snapshot();
    let mut iter = db.iter(&snapshot).unwrap();
    let mut count = 0;
    while let Some(Ok(entry)) = iter.next() {
        count += 1;
        assert_eq!(entry.key.as_ref(), "key1");
        assert_eq!(entry.value.value.as_ref().unwrap().as_ref(), "v2");
    }
    assert_eq!(count, 1, "Should only yield the latest version");
}

#[test]
fn test_merged_iterator_tombstone_filtering() {
    let (_tmp_dir, path) = setup();
    let db = DB::open(&path, 10 * 1024 * 1024).unwrap();

    db.put("key1".to_string(), "v1".to_string()).unwrap();
    db.delete("key1".to_string()).unwrap();

    let snapshot = db.snapshot();
    let mut iter = db.iter(&snapshot).unwrap();
    assert!(iter.next().is_none(), "Tombstone should be filtered out");
}

#[test]
fn test_merged_iterator_snapshot_isolation() {
    let (_tmp_dir, path) = setup();
    let db = DB::open(&path, 10 * 1024 * 1024).unwrap();

    db.put("key1".to_string(), "v1".to_string()).unwrap();

    let snapshot = db.snapshot();

    db.put("key1".to_string(), "v2".to_string()).unwrap();

    // Iterator with snapshot should see v1
    let mut snap_iter = db.iter(&snapshot).unwrap();
    let entry_snap = snap_iter.next().unwrap().unwrap();
    assert_eq!(entry_snap.value.value.as_ref().unwrap().as_ref(), "v1");

    // New snapshot for current view
    let current_snapshot = db.snapshot();
    let mut current_iter = db.iter(&current_snapshot).unwrap();
    let entry_curr = current_iter.next().unwrap().unwrap();
    assert_eq!(entry_curr.value.value.as_ref().unwrap().as_ref(), "v2");
}

#[test]
fn test_merged_iterator_lsm_merge() {
    let (_tmp_dir, path) = setup();
    // Small memtable to force flushes
    let db = DB::open(&path, 1024).unwrap();

    // Fill memtable to trigger flush
    for i in 0..100 {
        db.put(format!("key_{:03}", i), "val".to_string()).unwrap();
    }

    // Add more data to active memtable
    db.put("key_new".to_string(), "val_new".to_string())
        .unwrap();

    let snapshot = db.snapshot();
    let mut iter = db.iter(&snapshot).unwrap();
    let mut keys = Vec::new();
    while let Some(Ok(entry)) = iter.next() {
        keys.push(entry.key.clone());
    }

    assert!(keys.iter().any(|k| k.as_ref() == "key_new"));
    assert!(keys.len() >= 101);
    // Verify sortedness across layers
    for i in 0..keys.len() - 1 {
        assert!(keys[i] < keys[i + 1]);
    }
}
