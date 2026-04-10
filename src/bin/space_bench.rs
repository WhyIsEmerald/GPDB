use colored::*;
use gpdb::{MemTable, SSTable, SSTableId};
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use std::error::Error;
use std::fmt;
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

fn format_usize(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    let mut cnt = 0;
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

fn format_kb(bytes: u64) -> String {
    let kb = (bytes as f64) / 1024.0;
    let kb_cents = (kb * 100.0).round() as i64;
    let int_part = (kb_cents / 100) as usize;
    let frac = (kb_cents.abs() % 100) as usize;
    format!("{}.{}", format_usize(int_part), format!("{:02}", frac))
}

struct SpaceTestConfig {
    name: &'static str,
    key_prefix: Option<&'static str>,
    key_length: usize,
    num_entries: usize,
    value_length: usize,
    key_type: KeyType,
}

#[derive(Clone, Copy, Debug)]
enum KeyType {
    PrefixedSequential,
    Random,
    Sequential,
}

impl fmt::Display for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

fn generate_key(config: &SpaceTestConfig, idx: usize) -> String {
    match config.key_type {
        KeyType::PrefixedSequential => {
            let prefix = config.key_prefix.unwrap_or("");
            let width = config.key_length.saturating_sub(prefix.len());
            format!("{}{:0width$}", prefix, idx, width = width)
        }
        KeyType::Random => thread_rng()
            .sample_iter(&Alphanumeric)
            .take(config.key_length)
            .map(char::from)
            .collect(),
        KeyType::Sequential => format!("{:0width$}", idx, width = config.key_length),
    }
}

fn run_space_test(config: &SpaceTestConfig, tmp_dir: &TempDir) -> Result<(), Box<dyn Error>> {
    println!(
        "{}",
        format!("\n=== Running Test: {} ===", config.name).bold()
    );

    let path = tmp_dir.path();
    let mut mem = MemTable::new();
    let val = Arc::new("v".repeat(config.value_length));
    let mut total_raw_bytes: u64 = 0;

    for i in 0..config.num_entries {
        let key = generate_key(config, i);
        total_raw_bytes =
            total_raw_bytes.saturating_add(key.len() as u64 + config.value_length as u64);
        mem.put(key, val.clone());
    }

    let sstable_name = config.name.replace(' ', "_").to_lowercase() + ".sst";
    let sstable_path = path.join(&sstable_name);
    SSTable::write_from_memtable(&sstable_path, &mem, SSTableId(1))?;

    let metadata = fs::metadata(&sstable_path)?;
    let on_disk_bytes = metadata.len();
    let amp_factor = on_disk_bytes as f64 / total_raw_bytes as f64;

    println!(
        "{} {}",
        "Test Name:".bright_blue().bold(),
        config.name.white()
    );
    println!(
        "{} {}",
        "Key Type:".bright_blue().bold(),
        format!("{}", config.key_type).white()
    );
    if let Some(pfx) = config.key_prefix {
        println!("{} {}", "Key Prefix:".bright_blue().bold(), pfx.white());
    }
    println!(
        "{} {}",
        "Num Entries:".bright_blue().bold(),
        format_usize(config.num_entries).white()
    );
    println!(
        "{} {}",
        "Key Length:".bright_blue().bold(),
        format_usize(config.key_length).white()
    );
    println!(
        "{} {}",
        "Value Length:".bright_blue().bold(),
        format_usize(config.value_length).white()
    );
    println!(
        "{} {} KB",
        "Raw Data Ingested:".bright_blue().bold(),
        format_kb(total_raw_bytes)
    );
    println!(
        "{} {} KB",
        "SSTable on Disk:".bright_blue().bold(),
        format_kb(on_disk_bytes)
    );
    println!("{}", "--------------------------------------".dimmed());

    let amp_text = format!("{:>6.2}x", amp_factor);
    let status = if amp_factor < 1.05 {
        ("ELITE".green().bold(), amp_text.green())
    } else if amp_factor < 1.15 {
        ("EXCELLENT".yellow().bold(), amp_text.yellow())
    } else {
        ("GOOD".cyan().bold(), amp_text.cyan())
    };

    println!(
        "{} {}",
        "Space Amplification:".bright_blue().bold(),
        status.1
    );
    println!("{} {}", "Status:".bright_blue().bold(), status.0);

    println!(
        "{}",
        "--------------------------------------------------------------".dimmed()
    );
    println!();

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let tmp_dir = TempDir::new()?;
    println!("{}", "=== GPDB SPACE AMPLIFICATION ANALYSIS ===".bold());

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

    for config in &tests {
        if let Err(e) = run_space_test(config, &tmp_dir) {
            eprintln!("{} {}", "Test failed:".red().bold(), e.to_string().red());
        }
    }

    println!(
        "\n{}",
        "=== All Space Amplification Tests Completed ===".bold()
    );
    Ok(())
}
