use gpdb::DB;
use tempfile::TempDir;

#[test]
fn test_db_basic_ops() {
    let tmp_dir = TempDir::new().unwrap();
    let db: DB<String, String> = DB::open(tmp_dir.path(), 1024).expect("Failed to open DB");

    db.put("k1".to_string(), "v1".to_string()).unwrap();
    assert_eq!(db.get(&"k1".to_string()).unwrap().unwrap().as_str(), "v1");

    db.put("k1".to_string(), "v2".to_string()).unwrap();
    assert_eq!(db.get(&"k1".to_string()).unwrap().unwrap().as_str(), "v2");

    db.delete("k1".to_string()).unwrap();
    assert!(db.get(&"k1".to_string()).unwrap().is_none());

    assert!(db.get(&"k2".to_string()).unwrap().is_none());
}

#[test]
fn test_db_persistence_and_recovery() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    {
        let db: DB<String, String> = DB::open(path, 1024).unwrap();
        db.put("k1".to_string(), "v1".to_string()).unwrap();
        db.put("k2".to_string(), "v2".to_string()).unwrap();
        db.delete("k3".to_string()).unwrap();
    }

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
    let db: DB<String, String> = DB::open(tmp_dir.path(), 10).unwrap();

    db.put("very-long-key".to_string(), "value".to_string())
        .unwrap();

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

    let db: DB<String, String> = DB::open(path, 50).unwrap();

    for i in 0..10 {
        db.put(format!("key-{}", i), "val".to_string()).unwrap();
    }

    for i in 0..5 {
        db.put(format!("key-{}", i), "new-val".to_string()).unwrap();
    }

    db.delete("key-7".to_string()).unwrap();
    db.delete("key-8".to_string()).unwrap();

    for i in 0..5 {
        assert_eq!(
            db.get(&format!("key-{}", i)).unwrap().unwrap().as_str(),
            "new-val"
        );
    }
    assert!(db.get(&"key-7".to_string()).unwrap().is_none());

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

    {
        let db: DB<String, String> = DB::open(path, 20).unwrap();
        db.put("a".to_string(), "v0".to_string()).unwrap();
        db.put("b".to_string(), "v0".to_string()).unwrap();
        db.put("c".to_string(), "v0".to_string()).unwrap();
        db.put("d".to_string(), "v0".to_string()).unwrap();
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

#[test]
fn test_db_level_n_compaction() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    let db: DB<String, String> = DB::open(path, 50).unwrap();

    for i in 0..20 {
        db.put(format!("key-{}", i), "val".to_string()).unwrap();
    }

    let val = db.get(&"key-0".to_string()).unwrap().unwrap();
    assert_eq!(val.as_str(), "val");
}
