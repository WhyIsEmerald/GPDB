use gpdb::DB;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Standardized metrics for a single benchmark phase.
#[derive(Debug, Clone)]
struct Metrics {
    name: String,
    total_ops: usize,
    duration: Duration,
    total_bytes: u64,
    hits: Option<usize>,
}

impl Metrics {
    fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.duration.as_secs_f64()
    }

    fn latency_us(&self) -> f64 {
        (self.duration.as_secs_f64() * 1_000_000.0) / self.total_ops as f64
    }

    fn throughput_mbs(&self) -> f64 {
        (self.total_bytes as f64 / 1024.0 / 1024.0) / self.duration.as_secs_f64()
    }

    fn hit_rate(&self) -> Option<f64> {
        self.hits
            .map(|h| (h as f64 / self.total_ops as f64) * 100.0)
    }
}

struct Reporter {
    results: Vec<Metrics>,
    history: HashMap<String, f64>,
    log_file_path: String,
}

impl Reporter {
    fn new(path: &str) -> gpdb::Result<Self> {
        let mut history = HashMap::new();
        if Path::new(path).exists() {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.starts_with("Phase: ") {
                    let parts: Vec<&str> = line.split('|').collect();
                    if parts.len() >= 3 {
                        let name = parts[0].replace("Phase: ", "").trim().to_string();
                        if let Some(speed_part) = parts[2].split(':').nth(1) {
                            if let Ok(speed) = speed_part
                                .trim()
                                .split_whitespace()
                                .next()
                                .unwrap_or("0")
                                .parse::<f64>()
                            {
                                history.insert(name, speed);
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            results: Vec::new(),
            history,
            log_file_path: path.to_string(),
        })
    }

    fn record(&mut self, metrics: Metrics) -> gpdb::Result<()> {
        let name_padding = " ".repeat(25usize.saturating_sub(metrics.name.len()));
        let speed = metrics.ops_per_sec();

        let mut msg = format!(
            "Phase: {}{}| Ops: {:<10} | Speed: {:<12.0} ops/s | Latency: {:<8.2} μs/op | Throughput: {:<8.2} MB/s",
            metrics.name,
            name_padding,
            metrics.total_ops,
            speed,
            metrics.latency_us(),
            metrics.throughput_mbs()
        );

        if let Some(hr) = metrics.hit_rate() {
            msg.push_str(&format!(" | Hit Rate: {:.1}%", hr));
        }

        if let Some(prev_speed) = self.history.get(&metrics.name) {
            if *prev_speed > 0.0 {
                let diff = (speed - prev_speed) / prev_speed * 100.0;
                let color_code = if diff >= 0.0 { "\x1b[32m" } else { "\x1b[31m" };
                msg.push_str(&format!(" | {}{:+>6.1}%\x1b[0m vs prev", color_code, diff));
            }
        }

        println!("{}", msg);
        self.results.push(metrics);
        Ok(())
    }

    fn finalize(&mut self, db_path: &Path, raw_bytes: u64) -> gpdb::Result<()> {
        println!("\n=== IMPROVEMENT & EFFICIENCY ANALYSIS ===");
        let mut f = File::create(&self.log_file_path)?;

        for m in &self.results {
            let name_padding = " ".repeat(25usize.saturating_sub(m.name.len()));
            let msg = format!(
                "Phase: {}{}| Ops: {:<10} | Speed: {:<12.0} ops/s | Latency: {:<8.2} μs/op | Throughput: {:<8.2} MB/s",
                m.name,
                name_padding,
                m.total_ops,
                m.ops_per_sec(),
                m.latency_us(),
                m.throughput_mbs()
            );
            writeln!(f, "{}", msg)?;
        }

        if let (Some(before), Some(after)) = (
            self.results
                .iter()
                .find(|m| m.name == "Random Read (Dirty)"),
            self.results
                .iter()
                .find(|m| m.name == "Random Read (Cleaned)"),
        ) {
            let improvement =
                (before.latency_us() - after.latency_us()) / before.latency_us() * 100.0;
            let msg = format!(
                ">> Read Latency Improvement (via Compaction): {:.2}%",
                improvement
            );
            println!("{}", msg);
            writeln!(f, "{}", msg)?;
        }

        let on_disk = count_disk_usage(db_path);
        let amp = on_disk as f64 / raw_bytes as f64;
        let msg = format!(
            ">> Space Amplification Factor: {:.2}x (Raw: {:.2}MB, Disk: {:.2}MB)",
            amp,
            raw_bytes as f64 / 1024.0 / 1024.0,
            on_disk as f64 / 1024.0 / 1024.0
        );
        println!("{}", msg);
        writeln!(f, "{}", msg)?;

        Ok(())
    }
}

fn main() -> gpdb::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let mut reporter = Reporter::new("benchmark_results.txt")?;

    // Config: 1MB MemTable
    const MEMTABLE_SIZE: usize = 1024 * 1024;
    let mut db: DB<String, String> = DB::open(path, MEMTABLE_SIZE)?;

    println!("=== GPDB EXTREME STRESS TEST ===");
    println!("Target: {:?} | MemTable: 1MB\n", path);

    // --- PHASE 1: BULK INGESTION ---
    let num_writes = 1_000_000;
    let key_size = 14;
    let val_size = 34;
    let total_bytes = num_writes as u64 * (key_size + val_size);

    let start = Instant::now();
    for i in 0..num_writes {
        db.put(format!("key-{:010}", i), format!("val-{:030}", i))?;
    }
    reporter.record(Metrics {
        name: "Bulk Ingestion".to_string(),
        total_ops: num_writes,
        duration: start.elapsed(),
        total_bytes,
        hits: None,
    })?;

    // --- PHASE 2: RANDOM OVERWRITES ---
    let num_overwrites = 500_000;
    let start = Instant::now();
    for i in 0..num_overwrites {
        let target = (i * 1103515245 + 12345) % num_writes;
        db.put(
            format!("key-{:010}", target),
            format!("updated-val-{:022}", target),
        )?;
    }
    reporter.record(Metrics {
        name: "Random Overwrites".to_string(),
        total_ops: num_overwrites,
        duration: start.elapsed(),
        total_bytes: num_overwrites as u64 * (key_size + val_size),
        hits: None,
    })?;

    // --- PHASE 3: RANDOM READ (Dirty) ---
    let num_reads = 100_000;
    let (dirty_metrics, _) = run_read_phase(&db, "Random Read (Dirty)", num_reads, num_writes)?;
    reporter.record(dirty_metrics)?;

    // --- PHASE 4: MULTI-THREADED CONTENTION ---
    let threads_count = 4;
    let writes_per_thread = 50_000;
    let db_shared = Arc::new(Mutex::new(db));
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..threads_count {
        let db_clone = Arc::clone(&db_shared);
        handles.push(thread::spawn(move || {
            for i in 0..writes_per_thread {
                let mut db_lock = db_clone.lock().unwrap();
                db_lock
                    .put(
                        format!("t{}-key-{:010}", t, i),
                        format!("thread-val-{:020}", i),
                    )
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    reporter.record(Metrics {
        name: "Multi-Threaded Ingest".to_string(),
        total_ops: threads_count * writes_per_thread,
        duration: start.elapsed(),
        total_bytes: (threads_count * writes_per_thread) as u64 * (key_size + val_size),
        hits: None,
    })?;

    // Recover DB
    let mut db = Arc::try_unwrap(db_shared).unwrap().into_inner().unwrap();

    // --- PHASE 5: DELETIONS ---
    let num_deletes = 250_000;
    let start = Instant::now();
    for i in 0..num_deletes {
        db.delete(format!("key-{:010}", i))?;
    }
    reporter.record(Metrics {
        name: "Random Deletions".to_string(),
        total_ops: num_deletes,
        duration: start.elapsed(),
        total_bytes: num_deletes as u64 * key_size,
        hits: None,
    })?;

    // --- PHASE 6: COMPACTION CATCH-UP ---
    println!("\n[System] Waiting for compaction to catch up...");
    let start = Instant::now();
    let initial_ssts = count_sstables(path);
    for _ in 0..10 {
        db.handle_compaction_results()?;
        thread::sleep(Duration::from_millis(500));
    }
    let final_ssts = count_sstables(path);
    println!(
        "[System] Compaction reduced SSTables from {} to {}",
        initial_ssts, final_ssts
    );

    reporter.record(Metrics {
        name: "Compaction Wait".to_string(),
        total_ops: 1,
        duration: start.elapsed(),
        total_bytes: 0,
        hits: None,
    })?;

    // --- PHASE 7: RANDOM READ (Cleaned) ---
    let (clean_metrics, _) = run_read_phase(&db, "Random Read (Cleaned)", num_reads, num_writes)?;
    reporter.record(clean_metrics)?;

    // Final analysis
    let total_ingested_bytes = (num_writes + num_overwrites + (threads_count * writes_per_thread))
        as u64
        * (key_size + val_size);
    reporter.finalize(path, total_ingested_bytes)?;

    Ok(())
}

fn run_read_phase(
    db: &DB<String, String>,
    name: &str,
    num_reads: usize,
    range: usize,
) -> gpdb::Result<(Metrics, usize)> {
    let mut hits = 0;
    let start = Instant::now();
    for i in 0..num_reads {
        let target = (i * 1103515245 + 12345) % range;
        let key = format!("key-{:010}", target);
        if db.get(&key)?.is_some() {
            hits += 1;
        }
    }
    let metrics = Metrics {
        name: name.to_string(),
        total_ops: num_reads,
        duration: start.elapsed(),
        total_bytes: num_reads as u64 * 14,
        hits: Some(hits),
    };
    Ok((metrics, hits))
}

fn count_sstables(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
        .count()
}

fn count_disk_usage(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().unwrap().len())
        .sum()
}
