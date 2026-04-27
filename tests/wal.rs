use gpdb::{LogEntry, Wal, WalManager};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

fn setup() -> (TempDir, PathBuf) {
    let tmp_dir = TempDir::new().expect("Failed to create temporary directory");
    let path = tmp_dir.path().to_path_buf();
    (tmp_dir, path)
}

#[test]
fn wal_manager_rotation_and_delete() {
    let (_tmp_dir, path) = setup();
    let wm: WalManager<String, String> = WalManager::new(path.clone(), 0).unwrap();

    let entry = LogEntry::Put(Arc::new("k1".to_string()), Arc::new("v1".to_string()));
    wm.submit(Arc::new(vec![entry])).unwrap();

    assert!(path.join("000000.wal").exists());

    let old_id = wm.rotate().unwrap();
    assert_eq!(old_id, 0);
    assert!(path.join("000001.wal").exists());

    wm.delete(old_id).unwrap();
    assert!(!path.join("000000.wal").exists());
}

#[test]
fn wal_recovery_multiple_files() {
    let (_tmp_dir, path) = setup();

    {
        let wal0_path = path.join("000000.wal");
        let mut wal0: Wal<String, String> = Wal::create(&wal0_path).unwrap();
        wal0.append_batch(&[LogEntry::Put(
            Arc::new("k1".to_string()),
            Arc::new("v1".to_string()),
        )])
        .unwrap();
        wal0.flush().unwrap();

        let wal1_path = path.join("000001.wal");
        let mut wal1: Wal<String, String> = Wal::create(&wal1_path).unwrap();
        wal1.append_batch(&[LogEntry::Put(
            Arc::new("k2".to_string()),
            Arc::new("v2".to_string()),
        )])
        .unwrap();
        wal1.flush().unwrap();
    }

    // WalManager::new itself doesn't recover, the DB::open does.
    // But we can check if WalManager::new picks up the right ID.
    let wm: WalManager<String, String> = WalManager::new(path.clone(), 1).unwrap();
    let next_id = wm.rotate().unwrap();
    assert_eq!(next_id, 1);
    assert!(path.join("000002.wal").exists());
}
