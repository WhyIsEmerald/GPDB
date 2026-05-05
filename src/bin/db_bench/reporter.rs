use std::fs::File;
use std::io::Write;
use std::path::Path;
use crate::metrics::Metrics;
use colored::Colorize;

pub struct Reporter {
    pub results: Vec<Metrics>,
    pub log_file_path: String,
}

impl Reporter {
    pub fn new(path: &str) -> gpdb::Result<Self> {
        Ok(Self {
            results: Vec::new(),
            log_file_path: path.to_string(),
        })
    }

    pub fn record(&mut self, m: Metrics) -> gpdb::Result<()> {
        self.results.push(m.clone());
        println!("{}", format!("Recorded: {}", m.name).dimmed());
        Ok(())
    }

    pub fn finalize(&mut self, db_path: &Path, raw_bytes: u64) -> gpdb::Result<()> {
        use crate::utils::count_disk_usage;

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
        "{:<28} | {:>12} | {:>14} | {:>9} | {:>9} | {:>9} | {:>9} | {:>9} | {:>6} | {:>8} | {:>8} | {:>10} | {:>10} | {:>10} | {:>10} | {:>12}",
        "Phase",
        "Ops",
        "Speed (ops/s)",
        "Mean μs",
        "p50 μs",
        "p90 μs",
        "p95 μs",
        "p99 μs",
        "SSTs",
        "T p50",
        "T p99",
        "Accuracy(%)",
        "ReadImp(%)",
        "Δμs",
        "Cache Hit%",
        "Avoided I/O"
    );


        let mut f = File::create(&self.log_file_path)?;
        writeln!(f, "{}", header)?;
        println!("{}", header.bold());

        for m in &self.results {
            let phase = format!("{:<28}", m.name);
            let ops = format!("{:>12}", crate::utils::format_usize(m.total_ops as u64));
            let speed = format!("{:>14}", crate::utils::format_f64_short(m.ops_per_sec()));

            let (mean_s, p50_s, p90_s, p95_s, p99_s) = if let Some(ref l) = m.lat {
                let (a, b, c, d, e) = l.fmt_brief();
                (
                    format!("{:>9}", a),
                    format!("{:>9}", b),
                    format!("{:>9}", c),
                    format!("{:>9}", d),
                    format!("{:>9}", e),
                )
            } else {
                let mean = crate::utils::format_f64_short(m.mean_latency_us());
                (
                    format!("{:>9}", mean),
                    format!("{:>9}", "-"),
                    format!("{:>9}", "-"),
                    format!("{:>9}", "-"),
                    format!("{:>9}", "-"),
                )
            };

            let ssts = format!("{:>6}", crate::utils::format_usize(m.sst_count as u64));

            let (tp50, tp99) = if let Some(ref t) = m.touch {
                (
                    format!("{:>8}", crate::utils::format_f64_short(t.p50)),
                    format!("{:>8}", crate::utils::format_f64_short(t.p99)),
                )
            } else {
                (format!("{:>8}", "-"), format!("{:>8}", "-"))
            };

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
                "{:<28} | {:>12} | {:>14} | {:>9} | {:>9} | {:>9} | {:>9} | {:>9} | {:>6} | {:>8} | {:>8} | {:>10} | {:>10} | {:>10} | {:>10.2} | {:>12}",
                phase, ops, speed, mean_s, p50_s, p90_s, p95_s, p99_s, ssts, tp50, tp99, acc, ripct_s, abs_s, m.cache_hit_rate, crate::utils::format_usize(m.filter_avoided_io)
            );

            writeln!(f, "{}", line)?;
            println!("{}", line);
        }

        println!(
            "{} {} (Raw: {} MB, On-disk: {} MB)",
            "Space Amplification:".bold(),
            format!("{:.2}x", space_amp).bright_magenta(),
            crate::utils::format_f64_short(raw_bytes as f64 / 1024.0 / 1024.0),
            crate::utils::format_f64_short(on_disk as f64 / 1024.0 / 1024.0)
        );

        Ok(())
    }
}
