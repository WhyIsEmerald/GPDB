#[derive(Clone, Copy, Debug)]
pub enum KeyPattern {
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
    pub fn generate(&self, idx: usize, pad: usize) -> String {
        match *self {
            KeyPattern::Sequential => format!("key-{:0width$}", idx, width = pad),
            KeyPattern::SimpleRandom => {
                let r = crate::db_bench_impl::utils::lcg(idx);
                format!("key-{:0width$}", r % 1_000_000_000usize, width = pad)
            }
            KeyPattern::Striped(stripes) => {
                let stripe = idx % stripes;
                format!("s{}-key-{:0width$}", stripe, idx / stripes, width = pad)
            }
        }
    }
}

#[derive(Clone)]
pub struct BenchConfig {
    pub name: String,
    pub num_writes: usize,
    pub num_overwrites: usize,
    pub num_reads: usize,
    pub num_deletes: usize,
    pub memtable_size: usize,
    pub key_size: usize,
    pub val_size: usize,
    pub batch_size: usize,
    pub threads: usize,
    pub pattern: KeyPattern,
}
