use gpdb::DB;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use tempfile::TempDir;

#[test]
fn recovery_from_corrupted_wal() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    {
        let db: DB<String, String> = DB::open(path, 1024 * 1024).unwrap();
        db.put("k1".to_string(), "v1".to_string()).unwrap();
        db.put("k2".to_string(), "v2".to_string()).unwrap();
    }

    // Manually corrupt the WAL by flipping a bit in the middle
    let wal_path = path.join("wal.log");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .unwrap();
    file.seek(SeekFrom::Start(10)).unwrap(); // Somewhere in the first record
    file.write_all(&[0xFF]).unwrap();

    // Reopening should fail due to corruption
    let result: gpdb::Result<DB<String, String>> = DB::open(path, 1024 * 1024);
    assert!(result.is_err());
}

#[test]
fn recovery_from_partial_wal_write() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    {
        let db: DB<String, String> = DB::open(path, 1024 * 1024).unwrap();
        db.put("k1".to_string(), "v1".to_string()).unwrap();
    }

    // Truncate the WAL to simulate partial write at the end
    let wal_path = path.join("wal.log");
    let metadata = std::fs::metadata(&wal_path).unwrap();
    let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
    file.set_len(metadata.len() - 5).unwrap(); // Chop off last few bytes

    // Reopening should fail due to corruption (checksum mismatch or unexpected EOF)
    let result: gpdb::Result<DB<String, String>> = DB::open(path, 1024 * 1024);
    assert!(result.is_err());
}

#[test]
fn recovery_with_orphan_sstables() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    {
        let db: DB<String, String> = DB::open(path, 10).unwrap();
        db.put("k1".to_string(), "v1".to_string()).unwrap(); // Triggers flush
    }

    // Create a dummy SSTable file that is NOT in the manifest
    let orphan_path = path.join("L0-99999.sst");
    std::fs::File::create(&orphan_path).unwrap();

    // Reopening should succeed and ignore the orphan file
    let db: DB<String, String> = DB::open(path, 1024).unwrap();
    assert_eq!(db.get(&"k1".to_string()).unwrap().unwrap().as_str(), "v1");
    assert!(db.total_sst_count() == 1);
}
