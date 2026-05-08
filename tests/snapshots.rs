use gpdb::DB;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_temporal_isolation() {
    let tmp_dir = TempDir::new().unwrap();
    let db: DB<String, String> = DB::open(tmp_dir.path(), 1024).unwrap();

    db.put("key1".to_string(), "v1".to_string()).unwrap();
    let snapshot = db.snapshot();
    db.put("key1".to_string(), "v2".to_string()).unwrap();

    let res_snap = db.get(&"key1".to_string(), Some(&snapshot)).unwrap();
    assert_eq!(res_snap.value.unwrap().as_str(), "v1");

    let res_now = db.get(&"key1".to_string(), None).unwrap();
    assert_eq!(res_now.value.unwrap().as_str(), "v2");
}

#[test]
fn test_multi_snapshot() {
    let tmp_dir = TempDir::new().unwrap();
    let db: DB<String, String> = DB::open(tmp_dir.path(), 1024).unwrap();

    db.put("key1".to_string(), "v1".to_string()).unwrap();
    let snap1 = db.snapshot();

    db.put("key1".to_string(), "v2".to_string()).unwrap();
    let snap2 = db.snapshot();

    db.put("key1".to_string(), "v3".to_string()).unwrap();

    assert_eq!(
        db.get(&"key1".to_string(), Some(&snap1))
            .unwrap()
            .value
            .unwrap()
            .as_str(),
        "v1"
    );
    assert_eq!(
        db.get(&"key1".to_string(), Some(&snap2))
            .unwrap()
            .value
            .unwrap()
            .as_str(),
        "v2"
    );
    assert_eq!(
        db.get(&"key1".to_string(), None)
            .unwrap()
            .value
            .unwrap()
            .as_str(),
        "v3"
    );
}

#[test]
fn test_gc_protection() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let db: DB<String, String> = DB::open(path, 10).unwrap();

    db.put("gc_key".to_string(), "gc_val".to_string()).unwrap();

    for i in 0..100 {
        db.put(format!("fill-{}", i), "val".to_string()).unwrap();
    }

    let mut sst_file = None;
    let entries: Vec<_> = fs::read_dir(path).unwrap().collect();
    println!(
        "Files in dir: {:?}",
        entries
            .iter()
            .map(|e| e.as_ref().unwrap().path())
            .collect::<Vec<_>>()
    );
    for entry in entries {
        let entry = entry.unwrap();
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("sst") {
            sst_file = Some(p);
            break;
        }
    }
    let sst_path = sst_file.expect("SSTable should have been created");

    let snapshot = db.snapshot();

    db.put("gc_key".to_string(), "gc_val_new".to_string())
        .unwrap();

    for i in 100..1000 {
        db.put(format!("fill-{}", i), "val".to_string()).unwrap();
    }

    while db.compaction_backlog() > 0 {
        db.handle_compaction_results().unwrap();
    }

    assert!(sst_path.exists());

    drop(snapshot);

    assert!(!sst_path.exists());
}
