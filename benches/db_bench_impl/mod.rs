mod config;
mod metrics;
mod reporter;
mod utils;

use colored::Colorize;
use gpdb::DB;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use self::config::{BenchConfig, KeyPattern};
use self::metrics::{LatStats, Metrics, percentile};
use self::reporter::Reporter;

fn calculate_lat_stats(samples: Vec<u128>) -> Option<LatStats> {
    if samples.is_empty() {
        return None;
    }
    let cnt = samples.len();
    let sum: u128 = samples.iter().sum();
    let mean_us = (sum as f64) / (cnt as f64);
    let p50 = percentile(samples.clone(), 50.0);
    let p90 = percentile(samples.clone(), 90.0);
    let p95 = percentile(samples.clone(), 95.0);
    let p99 = percentile(samples.clone(), 99.0);
    Some(LatStats {
        mean_us,
        p50_us: p50,
        p90_us: p90,
        p95_us: p95,
        p99_us: p99,
    })
}

fn run_read_phase(
    db: &DB<String, String>,
    name: &str,
    num_reads: usize,
    range: usize,
    deleted_range: Option<usize>,
    pattern: KeyPattern,
    key_pad: usize,
) -> gpdb::Result<Metrics> {
    let mut correct = 0usize;
    let mut samples: Vec<u128> = Vec::with_capacity(num_reads);
    let mut touched_samples: Vec<u128> = Vec::with_capacity(num_reads);
    let start = Instant::now();

    for i in 0..num_reads {
        let target = utils::lcg(i).wrapping_mul(1103515245).wrapping_add(12345) % range;
        let key = pattern.generate(target, key_pad);

        let t0 = Instant::now();
        let read_res = db.get(&key)?;
        let val = read_res.value;
        let touched = read_res.sstables_touched;
        let dt = t0.elapsed();
        samples.push(dt.as_micros());
        touched_samples.push(touched as u128);

        match deleted_range {
            Some(dr) if target < dr => {
                if val.is_none() {
                    correct += 1;
                }
            }
            _ => {
                if val.is_some() {
                    correct += 1;
                }
            }
        }
    }

    let duration = start.elapsed();
    let accuracy = if num_reads > 0 {
        (correct as f64 / num_reads as f64) * 100.0
    } else {
        0.0
    };

    let avoided_io = db.bloom_filter_avoided_io();
    let (hits, misses) = db.block_cache_stats();
    let cache_hit_rate = if hits + misses > 0 {
        (hits as f64 / (hits + misses) as f64) * 100.0
    } else {
        0.0
    };

    let touch_stats = if !touched_samples.is_empty() {
        let sum: u128 = touched_samples.iter().sum();
        Some(self::metrics::TouchStats {
            mean: sum as f64 / touched_samples.len() as f64,
            p50: self::metrics::percentile(touched_samples.clone(), 50.0),
            p90: self::metrics::percentile(touched_samples.clone(), 90.0),
            p95: self::metrics::percentile(touched_samples.clone(), 95.0),
            p99: self::metrics::percentile(touched_samples.clone(), 99.0),
        })
    } else {
        None
    };

    Ok(Metrics {
        name: name.to_string(),
        total_ops: num_reads,
        duration,
        sst_count: db.total_sst_count(),
        accuracy,
        filter_avoided_io: avoided_io,
        lat: calculate_lat_stats(samples),
        touch: touch_stats,
        cache_hit_rate,
    })
}

fn run_full_benchmark(cfg: &BenchConfig) -> gpdb::Result<()> {
    println!();
    println!(
        "{}",
        "==============================================================".dimmed()
    );
    println!(
        "{}",
        format!("=== Running config: {} ===", cfg.name)
            .bold()
            .cyan()
    );
    println!(
        "{}",
        "--------------------------------------------------------------".dimmed()
    );

    fs::create_dir_all("logs")?;
    fs::create_dir_all("bench_data")?;

    let db_path = Path::new("bench_data").join(cfg.name.replace(' ', "_"));
    if db_path.exists() {
        fs::remove_dir_all(&db_path)?;
    }

    let db: DB<String, String> = DB::open(&db_path, cfg.memtable_size)?;
    let log_file = format!("logs/benchmark_{}.log", cfg.name.replace(' ', "_"));
    let mut reporter = Reporter::new(&log_file)?;

    let mut samples = Vec::with_capacity(cfg.num_writes);
    let start = Instant::now();
    for i in (0..cfg.num_writes).step_by(cfg.batch_size) {
        let mut batch = gpdb::WriteBatch::new();
        for j in 0..cfg.batch_size {
            let idx = i + j;
            if idx >= cfg.num_writes {
                break;
            }
            let key = cfg.pattern.generate(idx, cfg.key_size);
            let val = format!("v{:0width$}", idx, width = cfg.val_size);
            batch.put(key, val);
        }
        let t0 = Instant::now();
        db.write_batch(batch)?;
        samples.push(t0.elapsed().as_micros());
    }
    reporter.record(Metrics {
        name: "Batch Writes".into(),
        total_ops: cfg.num_writes,
        duration: start.elapsed(),
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
        filter_avoided_io: 0,
        lat: calculate_lat_stats(samples),
        touch: None,
        cache_hit_rate: 0.0,
    })?;

    if cfg.num_overwrites > 0 {
        let mut samples = Vec::with_capacity(cfg.num_overwrites);
        let start = Instant::now();
        for i in 0..cfg.num_overwrites {
            let target =
                utils::lcg(i).wrapping_mul(1664525).wrapping_add(1013904223) % cfg.num_writes;
            let key = cfg.pattern.generate(target, cfg.key_size);
            let val = format!("upd{:0width$}", target, width = cfg.val_size);
            let t0 = Instant::now();
            db.put(key, val)?;
            samples.push(t0.elapsed().as_micros());
        }
        reporter.record(Metrics {
            name: "Random Overwrites".into(),
            total_ops: cfg.num_overwrites,
            duration: start.elapsed(),
            sst_count: db.total_sst_count(),
            accuracy: 100.0,
            filter_avoided_io: 0,
            lat: calculate_lat_stats(samples),
            touch: None,
            cache_hit_rate: 0.0,
        })?;
    }

    if cfg.num_reads > 0 {
        let m = run_read_phase(
            &db,
            "Random Read (Dirty)",
            cfg.num_reads,
            cfg.num_writes,
            None,
            cfg.pattern,
            cfg.key_size,
        )?;
        reporter.record(m)?;
    }

    if cfg.threads > 1 {
        let threads = cfg.threads;
        let writes_per_thread = cfg.num_writes / threads;
        let start = Instant::now();
        let db_arc = Arc::new(db.clone());

        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let dbc = db_arc.clone();
            let pattern = cfg.pattern;
            let val_size = cfg.val_size;
            let key_pad = cfg.key_size;
            handles.push(thread::spawn(move || {
                let mut thread_samples = Vec::with_capacity(writes_per_thread);
                for i in 0..writes_per_thread {
                    let global = t * writes_per_thread + i;
                    let key = pattern.generate(global, key_pad);
                    let val = format!("thr{:0width$}", global, width = val_size);
                    let t0 = Instant::now();
                    dbc.put(key, val).unwrap();
                    thread_samples.push(t0.elapsed().as_micros());
                }
                thread_samples
            }));
        }

        let mut all_samples = Vec::with_capacity(threads * writes_per_thread);
        for h in handles {
            all_samples.extend(h.join().expect("thread panic"));
        }

        reporter.record(Metrics {
            name: "Multi-Threaded Batch Writes".into(),
            total_ops: threads * writes_per_thread,
            duration: start.elapsed(),
            sst_count: db.total_sst_count(),
            accuracy: 100.0,
            filter_avoided_io: 0,
            lat: calculate_lat_stats(all_samples),
            touch: None,
            cache_hit_rate: 0.0,
        })?;
    }

    if cfg.num_deletes > 0 {
        let mut samples = Vec::with_capacity(cfg.num_deletes);
        let start = Instant::now();
        for i in 0..cfg.num_deletes {
            let key = cfg.pattern.generate(i, cfg.key_size);
            let t0 = Instant::now();
            db.delete(key)?;
            samples.push(t0.elapsed().as_micros());
        }
        reporter.record(Metrics {
            name: "Random Deletions".into(),
            total_ops: cfg.num_deletes,
            duration: start.elapsed(),
            sst_count: db.total_sst_count(),
            accuracy: 100.0,
            filter_avoided_io: 0,
            lat: calculate_lat_stats(samples),
            touch: None,
            cache_hit_rate: 0.0,
        })?;
    }

    let start = Instant::now();
    let mut total_compacted = 0;
    while db.compaction_backlog() > 0 {
        total_compacted += db.handle_compaction_results()?;
        thread::sleep(Duration::from_millis(100));
        if start.elapsed() > Duration::from_secs(60) {
            println!("{}", "Compaction timeout reached".red());
            break;
        }
    }

    reporter.record(Metrics {
        name: "Compaction Wait".into(),
        total_ops: total_compacted,
        duration: start.elapsed(),
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
        filter_avoided_io: 0,
        lat: None,
        touch: None,
        cache_hit_rate: 0.0,
    })?;

    if cfg.num_reads > 0 {
        let m = run_read_phase(
            &db,
            "Random Read (Cleaned)",
            cfg.num_reads,
            cfg.num_writes,
            Some(cfg.num_deletes),
            cfg.pattern,
            cfg.key_size,
        )?;
        reporter.record(m)?;
    }

    let total_ingested =
        (cfg.num_writes + cfg.num_overwrites + cfg.threads * (cfg.num_writes / cfg.threads)) as u64
            * (cfg.key_size as u64 + cfg.val_size as u64);

    reporter.finalize(&db_path, total_ingested)?;
    fs::remove_dir_all(&db_path)?;
    Ok(())
}

pub fn run() -> gpdb::Result<()> {
    let mut configs = vec![
        BenchConfig {
            name: "SmallSeq".into(),
            num_writes: 50_000,
            num_overwrites: 10_000,
            num_reads: 20_000,
            num_deletes: 5_000,
            memtable_size: 4 * 1024 * 1024,
            key_size: 16,
            val_size: 64,
            batch_size: 1_000,
            threads: 1,
            pattern: KeyPattern::Sequential,
        },
        BenchConfig {
            name: "MediumRandom".into(),
            num_writes: 200_000,
            num_overwrites: 50_000,
            num_reads: 50_000,
            num_deletes: 25_000,
            memtable_size: 8 * 1024 * 1024,
            key_size: 24,
            val_size: 128,
            batch_size: 2_000,
            threads: 4,
            pattern: KeyPattern::SimpleRandom,
        },
        BenchConfig {
            name: "StripedHighConcurrency".into(),
            num_writes: 300_000,
            num_overwrites: 100_000,
            num_reads: 100_000,
            num_deletes: 50_000,
            memtable_size: 16 * 1024 * 1024,
            key_size: 32,
            val_size: 256,
            batch_size: 4_000,
            threads: 8,
            pattern: KeyPattern::Striped(16),
        },
        BenchConfig {
            name: "HighBatchIngest".into(),
            num_writes: 500_000,
            num_overwrites: 0,
            num_reads: 50_000,
            num_deletes: 0,
            memtable_size: 32 * 1024 * 1024,
            key_size: 32,
            val_size: 128,
            batch_size: 10_000,
            threads: 1,
            pattern: KeyPattern::Sequential,
        },
    ];

    if std::env::var("GPDB_QUICK_BENCH").is_ok() {
        println!(
            "{}",
            "Quick mode enabled: scaling workloads down".bright_yellow()
        );
        for cfg in &mut configs {
            cfg.num_writes = (cfg.num_writes / 10).max(1);
            cfg.num_overwrites /= 10;
            cfg.num_reads = (cfg.num_reads / 10).max(1);
            cfg.num_deletes /= 10;
            cfg.threads = cfg.threads.min(2);
        }
    }

    for cfg in &configs {
        if let Err(e) = run_full_benchmark(cfg) {
            eprintln!("{}", format!("Benchmark failed: {:?}", e).red());
        }
    }

    let _ = fs::remove_dir_all("bench_data");

    println!(
        "{}",
        "All benchmark configurations completed.".bold().green()
    );
    Ok(())
}
