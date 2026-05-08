use gpdb::{SSTable, SSTableId};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> gpdb::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    let memtable = gpdb::MemTable::new();
    let num_keys = 10_000;
    for i in 0..num_keys {
        memtable.put(
            Arc::new(format!("key-{}", i)),
            Arc::new("val".to_string()),
            0,
        );
    }

    let sst_path = path.join("fpr_test.sst");
    SSTable::write_from_memtable(&sst_path, &memtable, SSTableId(1), None).unwrap();
    let sst: SSTable<String, String> = SSTable::open(&sst_path, None).unwrap();

    let mut false_positives = 0;
    let test_reads = 100_000;

    println!(
        "Measuring Bloom Filter False Positive Rate for {} keys...",
        num_keys
    );
    let start = Instant::now();
    for i in num_keys..num_keys + test_reads {
        let key = format!("non-existent-key-{}", i).to_string();
        if sst.get(&key).unwrap().is_some() {
            false_positives += 1;
        }
    }
    let duration = start.elapsed();

    let fpr = (false_positives as f64 / test_reads as f64) * 100.0;
    println!("--- Results ---");
    println!("Keys in SSTable: {}", num_keys);
    println!("Non-existent reads: {}", test_reads);
    println!("False Positives: {}", false_positives);
    println!("Observed FPR: {:.4}%", fpr);
    println!("Time taken: {:?}", duration);

    Ok(())
}
