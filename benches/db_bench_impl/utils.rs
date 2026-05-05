use std::path::Path;

pub fn format_usize(n: u64) -> String {
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

pub fn format_f64_short(v: f64) -> String {
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
            format!("-{}", format_usize((-rounded) as u64))
        } else {
            format_usize(rounded as u64)
        }
    }
}

pub fn lcg(seed: usize) -> usize {
    let mut x = seed as u64;
    x = x.wrapping_mul(6364136223846793005u64).wrapping_add(1);
    x as usize
}

pub fn count_disk_usage(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap_or_else(|_| panic!("Failed to read dir {:?}", path))
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}
