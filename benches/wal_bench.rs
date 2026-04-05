use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use gpdb::{LogEntry, Wal};
use std::sync::Arc;
use tempfile::TempDir;

pub fn wal_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    group.sample_size(200); // Disk I/O is slow, 200 is sufficient

    let tmp_dir = TempDir::new().unwrap();
    let wal_path = tmp_dir.path().join("wal.log");
    let mut wal: Wal<String, String> = Wal::create(&wal_path).unwrap();

    // Benchmark different payload sizes
    for size in [100, 1024, 10240].iter() {
        let val = "a".repeat(*size);
        let entry = LogEntry::Put("key".to_string(), Arc::new(val));

        let mut count = 0;
        group.bench_with_input(BenchmarkId::new("append_no_flush", size), size, |b, _| {
            b.iter(|| {
                count += 1;
                if count % 1000 == 0 {
                    wal.clear().unwrap();
                }
                wal.append(black_box(&entry)).unwrap();
            })
        });

        let mut count = 0;
        group.bench_with_input(BenchmarkId::new("append_with_flush", size), size, |b, _| {
            b.iter(|| {
                count += 1;
                if count % 1000 == 0 {
                    wal.clear().unwrap();
                }
                wal.append(black_box(&entry)).unwrap();
                wal.flush().unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, wal_bench);
criterion_main!(benches);
