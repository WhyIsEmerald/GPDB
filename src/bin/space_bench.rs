use gpdb::{MemTable, SSTable, SSTableId};
use std::sync::Arc;
use tempfile::TempDir;

fn main() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    println!("=== GPDB SPACE AMPLIFICATION ANALYSIS ===");
    
    // Ingest 10,000 keys
    let num_entries = 10_000;
    let key_size = 14; // "key-0000000000"
    let val_size = 100; // "v" * 100
    let raw_data_bytes = num_entries as u64 * (key_size + val_size);

    let mut mem = MemTable::new();
    let val = Arc::new("v".repeat(100));
    
    for i in 0..num_entries {
        mem.put(format!("key-{:010}", i), val.clone());
    }

    let sstable_path = path.join("space_test.sst");
    let _ = SSTable::write_from_memtable(&sstable_path, &mem, SSTableId(1)).unwrap();
    
    let metadata = std::fs::metadata(&sstable_path).unwrap();
    let on_disk_bytes = metadata.len();

    let amp_factor = on_disk_bytes as f64 / raw_data_bytes as f64;

    println!("Raw Data Ingested:  {:>10.2} KB", raw_data_bytes as f64 / 1024.0);
    println!("SSTable on Disk:    {:>10.2} KB", on_disk_bytes as f64 / 1024.0);
    println!("--------------------------------------");
    println!("Space Amplification: {:>10.2}x", amp_factor);
    
    if amp_factor < 1.1 {
        println!("Status: EXCELLENT (Lean Format)");
    } else if amp_factor < 1.5 {
        println!("Status: GOOD (Standard Overhead)");
    } else {
        println!("Status: WARNING (High Metadata Overhead)");
    }
}
