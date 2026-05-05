use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LatStats {
    pub mean_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TouchStats {
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

impl LatStats {
    pub fn fmt_brief(&self) -> (String, String, String, String, String) {
        use crate::db_bench_impl::utils::{format_f64_short};
        (
            format_f64_short(self.mean_us),
            format_f64_short(self.p50_us),
            format_f64_short(self.p90_us),
            format_f64_short(self.p95_us),
            format_f64_short(self.p99_us),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Metrics {
    pub name: String,
    pub total_ops: usize,
    pub duration: Duration,
    pub sst_count: usize,
    pub accuracy: f64,
    pub filter_avoided_io: u64,
    pub lat: Option<LatStats>,
    pub touch: Option<TouchStats>,
    pub cache_hit_rate: f64,
}

impl Metrics {
    pub fn ops_per_sec(&self) -> f64 {
        if self.duration.as_secs_f64() <= 0.0 {
            return 0.0;
        }
        self.total_ops as f64 / self.duration.as_secs_f64()
    }

    pub fn mean_latency_us(&self) -> f64 {
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

pub fn percentile(mut samples: Vec<u128>, p: f64) -> f64 {
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
