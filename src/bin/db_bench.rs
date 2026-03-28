use gpdb::DB;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> gpdb::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();

    // Config: 2MB MemTable limit to force many SSTables and compactions
    let mut db: DB<String, String> = DB::open(path, 2 * 1024 * 1024)?;

    println!("--- GPDB Heavy Stress Test ---");
    println!("Target Directory: {:?}", path);
    println!("MemTable Limit:   2 MB");

    // 1. Write Benchmark (1 Million Records)
    let num_writes = 1_000_000;
    let key_size = 14;
    let val_size = 36;
    let total_bytes = num_writes as u64 * (key_size + val_size);

    println!("\nPhase 1: Writing {} records (Ingestion)...", num_writes);
    let start = Instant::now();
    for i in 0..num_writes {
        let key = format!("key-{:010}", i);
        let val = format!("value-{:030}", i);
        db.put(key, val)?;

        if i > 0 && i % 250_000 == 0 {
            println!("   ... {} records written", i);
        }
    }
    let duration = start.elapsed();

    let mbs = (total_bytes as f64 / 1024.0 / 1024.0) / duration.as_secs_f64();
    println!(">> Write Results:");
    println!("   - Duration:   {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", mbs);
    println!(
        "   - Speed:      {:.0} ops/sec",
        num_writes as f64 / duration.as_secs_f64()
    );

    println!("Waiting for background compaction to catch up...");
    std::thread::sleep(std::time::Duration::from_secs(5));

    // 2. Read Benchmark (Random Access)
    let num_reads = 50_000;
    println!(
        "\nPhase 2: Random Reading {} records (Disk + RAM)...",
        num_reads
    );
    let start = Instant::now();
    let mut hits = 0;
    for i in 0..num_reads {
        let target = (i * 7919) % num_writes;
        let key = format!("key-{:010}", target);
        if db.get(&key)?.is_some() {
            hits += 1;
        }
    }
    let duration = start.elapsed();

    println!(">> Read Results:");
    println!("   - Duration:   {:?}", duration);
    println!(
        "   - Latency:    {:.2} μs/op",
        (duration.as_secs_f64() * 1_000_000.0) / num_reads as f64
    );
    println!(
        "   - Throughput: {:.0} ops/sec",
        num_reads as f64 / duration.as_secs_f64()
    );
    println!("   - Hit Rate:   {}%", (hits * 100) / num_reads);

    // Count SSTables
    let sst_count = std::fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
        .count();
    println!(">> SSTables:   {} (Live on disk)", sst_count);

    Ok(())
}
