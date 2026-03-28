use gpdb::DB;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use tempfile::TempDir;
use std::thread;

fn main() -> gpdb::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    
    // Config: 2MB MemTable to force many background tasks
    let mut db: DB<String, String> = DB::open(path, 2 * 1024 * 1024)?;

    println!("=== GPDB INDUSTRIAL STRESS TEST ===");
    println!("Target: {:?} | MemTable: 2MB\n", path);

    // --- TEST 1: SINGLE-THREADED INGESTION (Baseline) ---
    let num_writes = 500_000;
    println!("[1/5] Baseline Ingestion: Writing {} records...", num_writes);
    let start = Instant::now();
    for i in 0..num_writes {
        db.put(format!("key-{:010}", i), format!("val-{:030}", i))?;
    }
    let duration = start.elapsed();
    print_write_stats(duration, num_writes, 44);

    // --- TEST 2: CLEAN READ LATENCY (Baseline) ---
    println!("\n[2/5] Baseline Read (Clean): Reading 50,000 records...");
    let read_start = Instant::now();
    let mut hits = 0;
    let num_reads = 50_000;
    for i in 0..num_reads {
        let target = (i * 7919) % num_writes;
        if db.get(&format!("key-{:010}", target))?.is_some() {
            hits += 1;
        }
    }
    let clean_dur = read_start.elapsed();
    let clean_latency = (clean_dur.as_secs_f64() * 1_000_000.0) / num_reads as f64;
    println!("   >> Latency: {:.2} μs/op | Throughput: {:.0} ops/sec (Hit Rate: {}%)", 
        clean_latency, num_reads as f64 / clean_dur.as_secs_f64(), (hits * 100) / num_reads);

    // --- TEST 3: MULTI-THREADED CONTENTION ---
    println!("\n[3/5] Multi-Threaded Pressure (4 Threads)...");
    let db_shared = Arc::new(Mutex::new(db));
    let threads_count = 4;
    let writes_per_thread = 100_000;
    let mut handles = vec![];
    
    let start = Instant::now();
    for t in 0..threads_count {
        let db_clone = Arc::clone(&db_shared);
        handles.push(thread::spawn(move || {
            for i in 0..writes_per_thread {
                let mut db = db_clone.lock().unwrap();
                db.put(format!("t{}-key-{:010}", t, i), format!("val-{:030}", i)).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    let duration = start.elapsed();
    print_write_stats(duration, threads_count * writes_per_thread, 44);

    let mut db = Arc::try_unwrap(db_shared).unwrap().into_inner().unwrap();

    // --- TEST 4: TOMBSTONE PRESSURE ---
    let tomb_count = 200_000;
    println!("\n[4/5] Deletion Stress: Deleting {} records...", tomb_count);
    let start = Instant::now();
    for i in 0..tomb_count {
        db.delete(format!("key-{:010}", i))?;
    }
    println!("   - Deletion took {:?}", start.elapsed());

    println!("   - Reading back keys (measuring tombstone interference)...");
    let read_start = Instant::now();
    let mut hits = 0;
    for i in 0..num_reads {
        let target = (i * 7919) % (num_writes - tomb_count) + tomb_count;
        if db.get(&format!("key-{:010}", target))?.is_some() {
            hits += 1;
        }
    }
    let tomb_dur = read_start.elapsed();
    let tomb_latency = (tomb_dur.as_secs_f64() * 1_000_000.0) / num_reads as f64;
    println!("   >> Latency: {:.2} μs/op | Throughput: {:.0} ops/sec (Hit Rate: {}%)", 
        tomb_latency, num_reads as f64 / tomb_dur.as_secs_f64(), (hits * 100) / num_reads);
    
    let penalty = (tomb_latency - clean_latency) / clean_latency * 100.0;
    println!("   >> Tombstone Performance Penalty: {:.1}%", penalty);

    // --- TEST 5: COMPACTION CATCH-UP ---
    println!("\n[5/5] Final Cleanup...");
    println!("   - Current SSTables: {}", count_sstables(path));
    println!("   - Waiting 5s for background worker...");
    thread::sleep(Duration::from_secs(5));
    db.handle_compaction_results()?;
    println!("   - Final SSTables:   {}", count_sstables(path));

    Ok(())
}

fn print_write_stats(dur: Duration, count: usize, record_size: u64) {
    let mbs = ((count as u64 * record_size) as f64 / 1024.0 / 1024.0) / dur.as_secs_f64();
    println!("   >> Throughput: {:.2} MB/s | Speed: {:.0} ops/sec", mbs, count as f64 / dur.as_secs_f64());
}

fn count_sstables(path: &std::path::Path) -> usize {
    std::fs::read_dir(path).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
        .count()
}
