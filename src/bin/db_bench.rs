use gpdb::DB;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Standardized metrics for a benchmark phase.
#[derive(Debug, Clone)]
struct Metrics {
    name: String,
    total_ops: usize,
    duration: Duration,
    hits: Option<usize>,
    sst_count: usize,
    accuracy: f64,
}

impl Metrics {
    fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.duration.as_secs_f64()
    }

    fn latency_us(&self) -> f64 {
        (self.duration.as_secs_f64() * 1_000_000.0) / self.total_ops as f64
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
            "Phase: {}{}| Ops: {:<10} | Speed: {:<12.0} ops/s | Latency: {:<8.2} μs/op | SSTs: {:<3}",
            metrics.name,
            name_padding,
            metrics.total_ops,
            speed,
            metrics.latency_us(),
            metrics.sst_count
        );

        if let Some(hr) = metrics.hit_rate() {
            msg.push_str(&format!(" | Hit: {:.1}%", hr));
        }

        msg.push_str(&format!(" | Accuracy: {:.1}%", metrics.accuracy));

        if let Some(prev_speed) = self.history.get(&metrics.name) {
            if *prev_speed > 0.0 {
                let diff = (speed - prev_speed) / prev_speed * 100.0;
                let color_code = if diff >= 0.0 { "\x1b[32m" } else { "\x1b[31m" };
                msg.push_str(&format!(" | {}{:6.1}% Speed\x1b[0m", color_code, diff));
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
                "Phase: {}{}| Ops: {:<10} | Speed: {:<12.0} ops/s | Latency: {:<8.2} μs/op | SSTs: {:<3}",
                m.name,
                name_padding,
                m.total_ops,
                m.ops_per_sec(),
                m.latency_us(),
                m.sst_count
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
            println!(
                ">> Read Latency Improvement (via Compaction): {:.2}%",
                improvement
            );
        }

        let on_disk = count_disk_usage(db_path);
        let amp = on_disk as f64 / raw_bytes as f64;
        println!(
            ">> Space Amplification Factor: {:.2}x (Raw: {:.2}MB, Disk: {:.2}MB)",
            amp,
            raw_bytes as f64 / 1024.0 / 1024.0,
            on_disk as f64 / 1024.0 / 1024.0
        );

        Ok(())
    }
}

fn main() -> gpdb::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let mut reporter = Reporter::new("benchmark_results.txt")?;

    // --- ENVIRONMENT TUNING ---
    let is_ci = std::env::var("GITHUB_ACTIONS").is_ok();
    let memtable_size = if is_ci { 4 * 1024 * 1024 } else { 1024 * 1024 };
    let num_writes = if is_ci { 500_000 } else { 5_000_000 };

    let db: DB<String, String> = DB::open(path, memtable_size)?;

    println!("=== GPDB OPTIMIZED STRESS TEST ===");
    println!(
        "Target: {:?} | MemTable: {}MB ({})\n",
        path,
        memtable_size / 1024 / 1024,
        if is_ci { "CI Mode" } else { "High Throughput Mode" }
    );

    // --- PHASE 1: BULK INGESTION ---
    let batch_size = 1_000;
    let key_size = 14;
    let val_size = 34;

    let start = Instant::now();
    for i in (0..num_writes).step_by(batch_size) {
        let mut batch = gpdb::WriteBatch::new();
        for j in 0..batch_size {
            let idx = i + j;
            batch.put(format!("key-{:010}", idx), format!("val-{:030}", idx));
        }
        db.write_batch(batch)?;
    }
    reporter.record(Metrics {
        name: "Bulk Ingestion".to_string(),
        total_ops: num_writes,
        duration: start.elapsed(),
        hits: None,
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
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
        hits: None,
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
    })?;

    // --- PHASE 3: RANDOM READ (Dirty) ---
    let num_reads = 100_000;
    let (dirty_metrics, _) =
        run_read_phase(&db, "Random Read (Dirty)", num_reads, num_writes, None)?;
    reporter.record(dirty_metrics)?;

    // --- PHASE 4: MULTI-THREADED CONTENTION ---
    let threads_count = 4;
    let writes_per_thread = 50_000;
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..threads_count {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..writes_per_thread {
                db_clone
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
        hits: None,
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
    })?;

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
        hits: None,
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
    })?;

    // --- PHASE 6: COMPACTION CATCH-UP ---
    println!("\n[System] Waiting for compaction to catch up...");
    let start = Instant::now();
    let initial_ssts = db.total_sst_count();

    loop {
        db.handle_compaction_results()?;
        if db.compaction_backlog() == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
        if start.elapsed() > Duration::from_secs(60) {
            println!("[System] Warning: Compaction timeout!");
            break;
        }
    }

    let final_ssts = db.total_sst_count();
    println!(
        "[System] Compaction reduced SSTables from {} to {}",
        initial_ssts, final_ssts
    );

    reporter.record(Metrics {
        name: "Compaction Wait".to_string(),
        total_ops: 1,
        duration: start.elapsed(),
        hits: None,
        sst_count: final_ssts,
        accuracy: 100.0,
    })?;

    // --- PHASE 7: RANDOM READ (Cleaned) ---
    let (clean_metrics, _) = run_read_phase(
        &db,
        "Random Read (Cleaned)",
        num_reads,
        num_writes,
        Some(num_deletes),
    )?;
    reporter.record(clean_metrics)?;

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
    delete_range: Option<usize>,
) -> gpdb::Result<(Metrics, usize)> {
    let mut hits = 0;
    let mut correct = 0;
    let start = Instant::now();
    for i in 0..num_reads {
        let target = (i * 1103515245 + 12345) % range;
        let key = format!("key-{:010}", target);
        let val = db.get(&key)?;

        match delete_range {
            Some(dr) if target < dr => {
                if val.is_none() {
                    correct += 1;
                }
            }
            _ => {
                if let Some(_) = val {
                    hits += 1;
                    correct += 1;
                }
            }
        }
    }

    let accuracy = (correct as f64 / num_reads as f64) * 100.0;
    let metrics = Metrics {
        name: name.to_string(),
        total_ops: num_reads,
        duration: start.elapsed(),
        hits: Some(hits),
        sst_count: db.total_sst_count(),
        accuracy,
    };
    Ok((metrics, hits))
}

fn count_disk_usage(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().unwrap().len())
        .sum()
}
