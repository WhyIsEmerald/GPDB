use gpdb::{LogEntry, Wal};
use std::path::PathBuf;
use std::{
    io::{Seek, Write},
    sync::Arc,
};
use tempfile::TempDir;

fn setup() -> (TempDir, PathBuf) {
    let tmp_dir = TempDir::new().expect("Failed to create temporary directory");
    let wal_path = tmp_dir.path().join("wal.log");
    (tmp_dir, wal_path)
}

#[test]
fn create_and_append_flush() {
    let (_tmp_dir, wal_path) = setup();
    let mut wal: Wal<String, String> = Wal::create(&wal_path).expect("Failed to create WAL");

    let key1 = "key1".to_string();
    let val1 = Arc::new("value1".to_string());
    let entry1 = LogEntry::Put(key1.clone(), val1.clone());
    wal.append(&entry1).expect("Failed to write log entry");

    let key2 = "key2".to_string();
    let val2 = Arc::new("value2".to_string());
    let entry2 = LogEntry::Put(key2.clone(), val2.clone());
    wal.append(&entry2).expect("Failed to write log entry");

    wal.flush().expect("Failed to flush WAL");

    let metadata = std::fs::metadata(&wal_path).expect("Failed to get WAL metadata");
    assert!(metadata.len() > 0);
}

#[test]
fn recovery_and_iter() {
    let (_tmp_dir, wal_path) = setup();

    {
        let mut wal: Wal<String, String> =
            Wal::create(&wal_path).expect("Failed to create WAL for writing");

        let entry1 = LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()));
        wal.append(&entry1).expect("Failed to write log entry");
        let entry2 = LogEntry::Delete("k2".to_string());
        wal.append(&entry2).expect("Failed to write log entry");
        let entry3 = LogEntry::Put("k3".to_string(), Arc::new("v3".to_string()));
        wal.append(&entry3).expect("Failed to write log entry");

        wal.flush().expect("Failed to flush WAL");
    }

    let wal: Wal<String, String> = Wal::open(&wal_path).expect("Failed to create Wal for writing");
    let mut wal_iter = wal.iter().expect("Failed to create WAL iterator");

    let entry1_read = wal_iter
        .next()
        .expect("Expected entry1")
        .expect("Entry1 read failed");
    assert_eq!(
        entry1_read,
        LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()))
    );

    let entry2_read = wal_iter
        .next()
        .expect("Expected entry2")
        .expect("Entry2 read failed");
    assert_eq!(entry2_read, LogEntry::Delete("k2".to_string()));

    let entry3_read = wal_iter
        .next()
        .expect("Expected entry3")
        .expect("Entry3 read failed");
    assert_eq!(
        entry3_read,
        LogEntry::Put("k3".to_string(), Arc::new("v3".to_string()))
    );

    assert!(wal_iter.next().is_none(), "Expected no more entries");
}

#[test]
fn clear() {
    let (_tmp_dir, wal_path) = setup();
    let mut wal: Wal<String, String> = Wal::create(&wal_path).expect("Failed to create WAL");
    let entry1 = LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()));
    let entry2 = LogEntry::Delete("k2".to_string());
    let entry3 = LogEntry::Put("k3".to_string(), Arc::new("v3".to_string()));

    wal.append(&entry1).expect("Failed to write log entry");
    wal.append(&entry2).expect("Failed to write log entry");
    wal.append(&entry3).expect("Failed to write log entry");

    wal.flush().expect("Failed to flush WAL");

    wal.clear().expect("Failed to clear WAL");

    let metadata = std::fs::metadata(&wal_path).expect("Failed to get WAL metadata");
    assert_eq!(metadata.len(), 0);
}

#[test]
fn corrupt_entry() {
    let (_tmp_dir, wal_path) = setup();

    {
        let mut wal: Wal<String, String> = Wal::create(&wal_path).expect("Failed to create WAL");

        let entry1 = LogEntry::Put("k1".to_string(), Arc::new("v1".to_string()));
        wal.append(&entry1).expect("Failed to write log entry");
        wal.flush().expect("Failed to flush WAL");
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .expect("Failed to open WAL file for corruption");
    file.seek(std::io::SeekFrom::Start(0))
        .expect("Failed to seek to start of WAL file");
    file.write_all(&[0x00, 0x00, 0x00, 0x00])
        .expect("Failed to corrupt checksum");

    let wal: Wal<String, String> = Wal::open(&wal_path).expect("Failed to open WAL for reading");
    let mut wal_iter = wal.iter().expect("Failed to create WAL iterator");
    let result = wal_iter
        .next()
        .expect("Expected an entry, potentially corrupted");
    assert!(
        result.is_err(),
        "Expected an error due to checksum mismatch"
    );
}
