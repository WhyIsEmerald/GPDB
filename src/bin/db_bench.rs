use colored::Colorize;
use gpdb::DB;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn format_usize(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    let mut cnt = 0usize;
    for c in s.chars().rev() {
        if cnt == 3 {
            out.push('_');
            cnt = 0;
        }
        out.push(c);
        cnt += 1;
    }
    out.chars().rev().collect()
}

fn format_f64_short(v: f64) -> String {
    if v.is_nan() || v.is_infinite() {
        return format!("{}", v);
    }
    if v.abs() < 1.0 {
        format!("{:.2}", v)
    } else if v < 1000.0 {
        format!("{:.1}", v)
    } else {
        let rounded = v.round() as i128;
        if rounded < 0 {
            format!("-{}", format_usize((-rounded) as usize))
        } else {
            format_usize(rounded as usize)
        }
    }
}

#[derive(Debug, Clone)]
struct LatStats {
    mean_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
}

impl LatStats {
    fn fmt_brief(&self) -> (String, String, String, String) {
        (
            format_f64_short(self.mean_us),
            format_f64_short(self.p50_us),
            format_f64_short(self.p95_us),
            format_f64_short(self.p99_us),
        )
    }
}

#[derive(Debug, Clone)]
struct Metrics {
    name: String,
    total_ops: usize,
    duration: Duration,
    sst_count: usize,
    accuracy: f64,
    lat: Option<LatStats>,
}

impl Metrics {
    fn ops_per_sec(&self) -> f64 {
        if self.duration.as_secs_f64() <= 0.0 {
            return 0.0;
        }
        self.total_ops as f64 / self.duration.as_secs_f64()
    }

    fn mean_latency_us(&self) -> f64 {
        if let Some(ref l) = self.lat {
            l.mean_us
        } else {
            if self.total_ops == 0 {
                0.0
            } else {
                (self.duration.as_secs_f64() * 1_000_000.0) / self.total_ops as f64
            }
        }
    }
}

struct Reporter {
    results: Vec<Metrics>,
    log_file_path: String,
}

impl Reporter {
    fn new(path: &str) -> gpdb::Result<Self> {
        Ok(Self {
            results: Vec::new(),
            log_file_path: path.to_string(),
        })
    }

    fn record(&mut self, m: Metrics) -> gpdb::Result<()> {
        self.results.push(m.clone());
        println!("{}", format!("Recorded: {}", m.name).dimmed());
        Ok(())
    }

    fn finalize(&mut self, db_path: &Path, raw_bytes: u64) -> gpdb::Result<()> {
        let dirty = self
            .results
            .iter()
            .find(|m| m.name == "Random Read (Dirty)")
            .cloned();
        let cleaned = self
            .results
            .iter()
            .find(|m| m.name == "Random Read (Cleaned)")
            .cloned();

        let (read_imp_pct, read_imp_abs_us, has_read_imp) =
            if let (Some(d), Some(c)) = (&dirty, &cleaned) {
                let dirty_mean = d.mean_latency_us();
                let clean_mean = c.mean_latency_us();
                if dirty_mean > 0.0 {
                    let pct = (dirty_mean - clean_mean) / dirty_mean * 100.0;
                    let abs = dirty_mean - clean_mean;
                    (pct, abs, true)
                } else {
                    (0.0, 0.0, false)
                }
            } else {
                (0.0, 0.0, false)
            };

        let on_disk = count_disk_usage(db_path);
        let space_amp = if raw_bytes > 0 {
            on_disk as f64 / raw_bytes as f64
        } else {
            0.0
        };

        let header = format!(
            "{:<28} | {:>12} | {:>14} | {:>9} | {:>9} | {:>9} | {:>9} | {:>6} | {:>10} | {:>10} | {:>10}",
            "Phase",
            "Ops",
            "Speed (ops/s)",
            "Mean μs",
            "p50 μs",
            "p95 μs",
            "p99 μs",
            "SSTs",
            "Accuracy(%)",
            "ReadImp(%)",
            "Δμs"
        );

        let mut f = File::create(&self.log_file_path)?;
        writeln!(f, "{}", header)?;
        println!("{}", header.bold());

        for m in &self.results {
            let phase = format!("{:<28}", m.name);
            let ops = format!("{:>12}", format_usize(m.total_ops));
            let speed = format!("{:>14}", format_f64_short(m.ops_per_sec()));

            let (mean_s, p50_s, p95_s, p99_s) = if let Some(ref l) = m.lat {
                let (a, b, c, d) = l.fmt_brief();
                (
                    format!("{:>9}", a),
                    format!("{:>9}", b),
                    format!("{:>9}", c),
                    format!("{:>9}", d),
                )
            } else {
                let mean = format_f64_short(m.mean_latency_us());
                (
                    format!("{:>9}", mean),
                    format!("{:>9}", "-"),
                    format!("{:>9}", "-"),
                    format!("{:>9}", "-"),
                )
            };

            let ssts = format!("{:>6}", format_usize(m.sst_count));
            let acc = format!("{:>10.1}", m.accuracy);

            let (ripct_s, abs_s) = if has_read_imp
                && (m.name == "Random Read (Dirty)" || m.name == "Random Read (Cleaned)")
            {
                (
                    format!("{:>10.2}", read_imp_pct),
                    format!("{:>10.2}", read_imp_abs_us),
                )
            } else {
                (format!("{:>10}", "-"), format!("{:>10}", "-"))
            };

            let line = format!(
                "{:<28} | {:>12} | {:>14} | {:>9} | {:>9} | {:>9} | {:>9} | {:>6} | {:>10} | {:>10} | {:>10}",
                phase, ops, speed, mean_s, p50_s, p95_s, p99_s, ssts, acc, ripct_s, abs_s
            );

            writeln!(f, "{}", line)?;
            println!("{}", line);
        }

        println!(
            "{} {} (Raw: {} MB, On-disk: {} MB)",
            "Space Amplification:".bold(),
            format!("{:.2}x", space_amp).bright_magenta(),
            format_f64_short(raw_bytes as f64 / 1024.0 / 1024.0),
            format_f64_short(on_disk as f64 / 1024.0 / 1024.0)
        );

        Ok(())
    }
}

#[derive(Clone)]
struct BenchConfig {
    name: String,
    num_writes: usize,
    num_overwrites: usize,
    num_reads: usize,
    num_deletes: usize,
    memtable_size: usize,
    key_size: usize,
    val_size: usize,
    batch_size: usize,
    threads: usize,
    pattern: KeyPattern,
}

#[derive(Clone, Copy, Debug)]
enum KeyPattern {
    Sequential,
    SimpleRandom,
    Striped(usize),
}

use std::fmt;
impl fmt::Display for KeyPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KeyPattern::Sequential => write!(f, "Sequential"),
            KeyPattern::SimpleRandom => write!(f, "SimpleRandom"),
            KeyPattern::Striped(s) => write!(f, "Striped({})", s),
        }
    }
}

impl KeyPattern {
    fn generate(&self, idx: usize, pad: usize) -> String {
        match *self {
            KeyPattern::Sequential => format!("key-{:0width$}", idx, width = pad),
            KeyPattern::SimpleRandom => {
                let r = lcg(idx);
                format!("key-{:0width$}", r % 1_000_000_000usize, width = pad)
            }
            KeyPattern::Striped(stripes) => {
                let stripe = idx % stripes;
                format!("s{}-key-{:0width$}", stripe, idx / stripes, width = pad)
            }
        }
    }
}

fn lcg(seed: usize) -> usize {
    let mut x = seed as u64;
    x = x.wrapping_mul(6364136223846793005u64).wrapping_add(1);
    x as usize
}

fn count_disk_usage(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap_or_else(|_| panic!("Failed to read dir {:?}", path))
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn percentile(mut samples: Vec<u128>, p: f64) -> f64 {
    let n = samples.len();
    if n == 0 {
        return 0.0;
    }
    samples.sort_unstable();
    let rank = (p / 100.0) * ((n - 1) as f64);
    let idx_low = rank.floor() as usize;
    let idx_high = rank.ceil() as usize;
    if idx_low == idx_high {
        samples[idx_low] as f64
    } else {
        let low = samples[idx_low] as f64;
        let high = samples[idx_high] as f64;
        let frac = rank - (idx_low as f64);
        low + (high - low) * frac
    }
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
    let start = Instant::now();

    for i in 0..num_reads {
        let target = lcg(i).wrapping_mul(1103515245).wrapping_add(12345) % range;
        let key = pattern.generate(target, key_pad);

        let t0 = Instant::now();
        let val = db.get(&key)?;
        let dt = t0.elapsed();
        let us = dt.as_micros();
        samples.push(us);

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

    let lat = if samples.is_empty() {
        None
    } else {
        let cnt = samples.len();
        let sum: u128 = samples.iter().sum();
        let mean_us = (sum as f64) / (cnt as f64);
        let p50 = percentile(samples.clone(), 50.0);
        let p95 = percentile(samples.clone(), 95.0);
        let p99 = percentile(samples.clone(), 99.0);
        Some(LatStats {
            mean_us,
            p50_us: p50,
            p95_us: p95,
            p99_us: p99,
        })
    };

    let metrics = Metrics {
        name: name.to_string(),
        total_ops: num_reads,
        duration,
        sst_count: db.total_sst_count(),
        accuracy,
        lat,
    };
    Ok(metrics)
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

    let tmp_dir = TempDir::new().expect("tempdir");
    let db: DB<String, String> = DB::open(tmp_dir.path(), cfg.memtable_size)?;

    let log_file = format!("benchmark_{}.log", cfg.name.replace(' ', "_"));
    let mut reporter = Reporter::new(&log_file)?;

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
        db.write_batch(batch)?;
    }
    reporter.record(Metrics {
        name: "Bulk Ingestion".into(),
        total_ops: cfg.num_writes,
        duration: start.elapsed(),
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
        lat: None,
    })?;

    if cfg.num_overwrites > 0 {
        let start = Instant::now();
        for i in 0..cfg.num_overwrites {
            let target = lcg(i).wrapping_mul(1664525).wrapping_add(1013904223) % cfg.num_writes;
            let key = cfg.pattern.generate(target, cfg.key_size);
            let val = format!("upd{:0width$}", target, width = cfg.val_size);
            db.put(key, val)?;
        }
        reporter.record(Metrics {
            name: "Random Overwrites".into(),
            total_ops: cfg.num_overwrites,
            duration: start.elapsed(),
            sst_count: db.total_sst_count(),
            accuracy: 100.0,
            lat: None,
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
                for i in 0..writes_per_thread {
                    let global = t * writes_per_thread + i;
                    let key = pattern.generate(global, key_pad);
                    let val = format!("thr{:0width$}", global, width = val_size);
                    dbc.put(key, val).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().expect("thread panic");
        }

        reporter.record(Metrics {
            name: "Multi-Threaded Ingest".into(),
            total_ops: threads * writes_per_thread,
            duration: start.elapsed(),
            sst_count: db.total_sst_count(),
            accuracy: 100.0,
            lat: None,
        })?;
    }

    if cfg.num_deletes > 0 {
        let start = Instant::now();
        for i in 0..cfg.num_deletes {
            let key = cfg.pattern.generate(i, cfg.key_size);
            db.delete(key)?;
        }
        reporter.record(Metrics {
            name: "Random Deletions".into(),
            total_ops: cfg.num_deletes,
            duration: start.elapsed(),
            sst_count: db.total_sst_count(),
            accuracy: 100.0,
            lat: None,
        })?;
    }

    let start = Instant::now();
    while db.compaction_backlog() > 0 {
        db.handle_compaction_results()?;
        thread::sleep(Duration::from_millis(100));
        if start.elapsed() > Duration::from_secs(60) {
            println!("{}", "Compaction timeout reached".red());
            break;
        }
    }

    reporter.record(Metrics {
        name: "Compaction Wait".into(),
        total_ops: 1,
        duration: start.elapsed(),
        sst_count: db.total_sst_count(),
        accuracy: 100.0,
        lat: None,
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

    reporter.finalize(tmp_dir.path(), total_ingested)?;
    Ok(())
}

fn main() -> gpdb::Result<()> {
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
    ];

    if std::env::var("GPDB_QUICK_BENCH").is_ok() {
        println!(
            "{}",
            "Quick mode enabled: scaling workloads down".bright_yellow()
        );
        for cfg in &mut configs {
            cfg.num_writes = (cfg.num_writes / 10).max(1);
            cfg.num_overwrites = (cfg.num_overwrites / 10).max(0);
            cfg.num_reads = (cfg.num_reads / 10).max(1);
            cfg.num_deletes = (cfg.num_deletes / 10).max(0);
            cfg.threads = cfg.threads.min(2);
        }
    }

    for cfg in &configs {
        if let Err(e) = run_full_benchmark(cfg) {
            eprintln!("{}", format!("Benchmark failed: {:?}", e).red());
        }
    }

    println!(
        "{}",
        "All benchmark configurations completed.".bold().green()
    );
    Ok(())
}
