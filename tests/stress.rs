use gpdb::DB;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

#[test]
fn concurrent_write_read_stress() {
    let tmp_dir = TempDir::new().unwrap();
    let db: DB<String, String> = DB::open(tmp_dir.path(), 4096).unwrap();

    let num_threads = 8;
    let ops_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..ops_per_thread {
                let key = format!("thread-{}-key-{}", t, i);
                let val = format!("val-{}", i);
                db.put(key, val).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all data
    for t in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("thread-{}-key-{}", t, i);
            let val = db.get(&key).unwrap().expect("Key missing");
            assert_eq!(val.as_str(), format!("val-{}", i));
        }
    }
}

#[test]
fn compaction_and_concurrent_get() {
    let tmp_dir = TempDir::new().unwrap();
    // Small memtable to trigger frequent flushes and compactions
    let db: DB<String, String> = DB::open(tmp_dir.path(), 512).unwrap();

    let num_keys = 2000;
    for i in 0..num_keys {
        db.put(format!("key-{}", i), "v1".to_string()).unwrap();
    }

    let db_clone = db.clone();
    let reader_handle = thread::spawn(move || {
        for _ in 0..100 {
            for i in 0..num_keys {
                let key = format!("key-{}", i);
                let _ = db_clone.get(&key).unwrap();
            }
        }
    });

    // Overwrite keys while reading to trigger more compactions
    for i in 0..num_keys {
        db.put(format!("key-{}", i), "v2".to_string()).unwrap();
    }

    reader_handle.join().unwrap();
}
