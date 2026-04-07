use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gpdb::MemTable;
use std::sync::Arc;

pub fn memtable_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable");
    group.sample_size(1000);

    let val = Arc::new("value-000000000000000000000000000000".to_string());

    group.bench_function("put_30b", |b| {
        let mut mem = MemTable::new();
        b.iter(|| {
            mem.put(
                black_box("key-0000000000".to_string()),
                black_box(val.clone()),
            );
        })
    });

    group.bench_function("get_hit", |b| {
        let mut mem = MemTable::new();
        mem.put("key-target".to_string(), val.clone());
        b.iter(|| {
            mem.get(black_box(&"key-target".to_string()));
        })
    });

    group.finish();
}

criterion_group!(benches, memtable_bench);
criterion_main!(benches);
