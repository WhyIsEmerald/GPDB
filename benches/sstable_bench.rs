use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gpdb::{MemTable, SSTable, SSTableId};
use std::sync::Arc;
use tempfile::TempDir;

pub fn sstable_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable");
    group.sample_size(200);

    let tmp_dir = TempDir::new().unwrap();
    let sstable_path = tmp_dir.path().join("bench.sst");

    let mut mem = MemTable::new();
    for i in 0..1000 {
        mem.put(format!("key-{:05}", i), Arc::new(format!("value-{}", i)));
    }

    let sst = SSTable::write_from_memtable(&sstable_path, &mem, SSTableId(1)).unwrap();

    group.bench_function("get_hit", |b| {
        b.iter(|| {
            sst.get(black_box(&"key-00500".to_string())).unwrap();
        })
    });

    group.bench_function("get_miss", |b| {
        b.iter(|| {
            sst.get(black_box(&"key-99999".to_string())).unwrap();
        })
    });

    group.bench_function("full_scan", |b| {
        b.iter(|| {
            let count = sst.iter().unwrap().count();
            black_box(count);
        })
    });

    group.finish();
}

criterion_group!(benches, sstable_bench);
criterion_main!(benches);
