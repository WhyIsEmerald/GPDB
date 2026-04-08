use gpdb::{MemTable, SSTable, SSTableId};
use rand::{Rng, thread_rng};
use std::sync::Arc;
use tempfile::TempDir;

/// Represents the configuration for a single space amplification test.
struct SpaceTestConfig {
    name: &'static str,
    key_prefix: Option<&'static str>,
    key_length: usize,
    num_entries: usize,
    value_length: usize,
    key_type: KeyType,
}

enum KeyType {
    /// Keys with a common prefix and a sequential suffix.
    PrefixedSequential,
    /// Fully random keys.
    Random,
    /// Keys that are purely sequential numbers, padded to key_length.
    Sequential,
}

/// Runs a single space amplification test based on the provided configuration.
fn run_space_test(config: &SpaceTestConfig, tmp_dir: &TempDir) {
    println!("\n=== Running Test: {} ===", config.name);

    let path = tmp_dir.path();
    let mut mem = MemTable::new();
    let val = Arc::new("v".repeat(config.value_length));
    let mut total_raw_bytes = 0;

    for i in 0..config.num_entries {
        let key = match config.key_type {
            KeyType::PrefixedSequential => {
                let prefix = config.key_prefix.unwrap_or("");
                format!(
                    "{}{:0width$}",
                    prefix,
                    i,
                    width = config.key_length - prefix.len()
                )
            }
            KeyType::Random => {
                let charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                let mut key = String::with_capacity(config.key_length);
                let mut rng = thread_rng();
                for _ in 0..config.key_length {
                    let idx = rng.gen_range(0..charset.len());
                    key.push(charset.chars().nth(idx).unwrap());
                }
                key
            }
            KeyType::Sequential => {
                format!("{:0width$}", i, width = config.key_length)
            }
        };

        total_raw_bytes += key.len() as u64 + config.value_length as u64;
        mem.put(key, val.clone());
    }

    let sstable_path = path.join(format!(
        "{}.sst",
        config.name.replace(" ", "_").to_lowercase()
    ));
    let _ = SSTable::write_from_memtable(&sstable_path, &mem, SSTableId(1)).unwrap();

    let metadata = std::fs::metadata(&sstable_path).unwrap();
    let on_disk_bytes = metadata.len();

    let amp_factor = on_disk_bytes as f64 / total_raw_bytes as f64;

    println!("  Test Name:          {}", config.name);
    println!("  Key Type:           {:?}", config.key_type);
    println!("  Num Entries:        {}", config.num_entries);
    println!("  Key Length:         {}", config.key_length);
    println!("  Value Length:       {}", config.value_length);
    println!(
        "  Raw Data Ingested:  {:>10.2} KB",
        total_raw_bytes as f64 / 1024.0
    );
    println!(
        "  SSTable on Disk:    {:>10.2} KB",
        on_disk_bytes as f64 / 1024.0
    );
    println!("--------------------------------------");
    println!("  Space Amplification: {:>10.2}x", amp_factor);

    if amp_factor < 1.05 {
        println!("  Status: ELITE");
    } else if amp_factor < 1.15 {
        println!("  Status: EXCELLENT");
    } else {
        println!("  Status: GOOD");
    }
}

// Implement Debug for KeyType for printing
impl std::fmt::Debug for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyType::PrefixedSequential => write!(f, "PrefixedSequential"),
            KeyType::Random => write!(f, "Random"),
            KeyType::Sequential => write!(f, "Sequential"),
        }
    }
}

fn main() {
    let tmp_dir = TempDir::new().unwrap();

    println!("=== GPDB 'ELITE' SPACE AMPLIFICATION ANALYSIS ===");

    let tests = vec![
        SpaceTestConfig {
            name: "Long Prefixed Keys (Delta Encoding Favored)",
            key_prefix: Some("com.github.whyisemerald.gpdb.records.user_id_"),
            key_length: 45,
            num_entries: 10_000,
            value_length: 100,
            key_type: KeyType::PrefixedSequential,
        },
        SpaceTestConfig {
            name: "Short Prefixed Keys",
            key_prefix: Some("user_"),
            key_length: 10,
            num_entries: 50_000,
            value_length: 50,
            key_type: KeyType::PrefixedSequential,
        },
        SpaceTestConfig {
            name: "Long Random Keys (No Delta Encoding)",
            key_prefix: None,
            key_length: 64,
            num_entries: 5_000,
            value_length: 200,
            key_type: KeyType::Random,
        },
        SpaceTestConfig {
            name: "Short Random Keys",
            key_prefix: None,
            key_length: 16,
            num_entries: 20_000,
            value_length: 20,
            key_type: KeyType::Random,
        },
        SpaceTestConfig {
            name: "Sequential Numeric Keys (Fixed Length)",
            key_prefix: None,
            key_length: 10,
            num_entries: 100_000,
            value_length: 10,
            key_type: KeyType::Sequential,
        },
        SpaceTestConfig {
            name: "Mixed Length Keys (Prefixed)",
            key_prefix: Some("data_"),
            key_length: 25,
            num_entries: 7_500,
            value_length: 75,
            key_type: KeyType::PrefixedSequential,
        },
    ];

    for config in tests {
        run_space_test(&config, &tmp_dir);
    }

    println!("\n=== All Space Amplification Tests Completed ===");
}
