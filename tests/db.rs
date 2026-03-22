use gpdb::DB;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_db_basic_ops() {
    let tmp_dir = TempDir::new().unwrap();
    let mut db: DB<String, String> = DB::open(tmp_dir.path(), 1024).expect("Failed to open DB");

    // Put & Get
    db.put("k1".to_string(), "v1".to_string()).unwrap();
    assert_eq!(db.get(&"k1".to_string()).unwrap().unwrap().as_str(), "v1");

    // Overwrite
    db.put("k1".to_string(), "v2".to_string()).unwrap();
    assert_eq!(db.get(&"k1".to_string()).unwrap().unwrap().as_str(), "v2");

    // Delete
    db.delete("k1".to_string()).unwrap();
    assert!(db.get(&"k1".to_string()).unwrap().is_none());

    // Non-existent key
    assert!(db.get(&"k2".to_string()).unwrap().is_none());
}

#[test]
fn test_db_persistence_and_recovery() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    {
        let mut db: DB<String, String> = DB::open(path, 1024).unwrap();
        db.put("k1".to_string(), "v1".to_string()).unwrap();
        db.put("k2".to_string(), "v2".to_string()).unwrap();
        db.delete("k3".to_string()).unwrap(); // Tombstone in WAL
    } // DB dropped, WAL remains

    let db_recovered: DB<String, String> = DB::open(path, 1024).unwrap();
    assert_eq!(
        db_recovered
            .get(&"k1".to_string())
            .unwrap()
            .unwrap()
            .as_str(),
        "v1"
    );
    assert_eq!(
        db_recovered
            .get(&"k2".to_string())
            .unwrap()
            .unwrap()
            .as_str(),
        "v2"
    );
    assert!(db_recovered.get(&"k3".to_string()).unwrap().is_none());
}

#[test]
fn test_db_flush_and_read() {
    let tmp_dir = TempDir::new().unwrap();
    // Tiny max size to force flush
    let mut db: DB<String, String> = DB::open(tmp_dir.path(), 10).unwrap();

    db.put("very-long-key".to_string(), "value".to_string())
        .unwrap();

    // After this, it should be in an SSTable (L0)
    assert_eq!(
        db.get(&"very-long-key".to_string())
            .unwrap()
            .unwrap()
            .as_str(),
        "value"
    );
}

#[test]
fn test_db_compaction_integration() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    // Max size 50 bytes to force frequent flushes
    let mut db: DB<String, String> = DB::open(path, 50).unwrap();

    // Write enough to trigger multiple flushes
    for i in 0..10 {
        db.put(format!("key-{}", i), "val".to_string()).unwrap();
    }

    // Overwrite some keys
    for i in 0..5 {
        db.put(format!("key-{}", i), "new-val".to_string()).unwrap();
    }

    // Delete some keys
    db.delete("key-7".to_string()).unwrap();
    db.delete("key-8".to_string()).unwrap();

    // Verify values (should trigger compaction eventually)
    for i in 0..5 {
        assert_eq!(
            db.get(&format!("key-{}", i)).unwrap().unwrap().as_str(),
            "new-val"
        );
    }
    assert!(db.get(&"key-7".to_string()).unwrap().is_none());

    // Close and Reopen to verify Manifest state
    drop(db);
    let db_reopened: DB<String, String> = DB::open(path, 50).unwrap();

    assert_eq!(
        db_reopened
            .get(&"key-0".to_string())
            .unwrap()
            .unwrap()
            .as_str(),
        "new-val"
    );
    assert!(db_reopened.get(&"key-7".to_string()).unwrap().is_none());
}

#[test]
fn test_db_manifest_complex_reconciliation() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    // Create a situation with multiple levels and overlapping keys
    {
        let mut db: DB<String, String> = DB::open(path, 20).unwrap();
        // L0 files
        db.put("a".to_string(), "v0".to_string()).unwrap();
        db.put("b".to_string(), "v0".to_string()).unwrap();
        db.put("c".to_string(), "v0".to_string()).unwrap();
        db.put("d".to_string(), "v0".to_string()).unwrap();
        // This triggers L0->L1 compaction
        db.put("e".to_string(), "v0".to_string()).unwrap();
    }

    let db_reopened: DB<String, String> = DB::open(path, 1024).unwrap();
    assert_eq!(
        db_reopened.get(&"a".to_string()).unwrap().unwrap().as_str(),
        "v0"
    );
    assert_eq!(
        db_reopened.get(&"e".to_string()).unwrap().unwrap().as_str(),
        "v0"
    );
}
