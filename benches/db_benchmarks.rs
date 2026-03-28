use criterion::{Criterion, criterion_group, criterion_main};
use gpdb::MemTable;
use std::sync::Arc;

pub fn memtable_bench(c: &mut Criterion) {
    let mut mem = MemTable::new();
    let val = Arc::new("value".to_string());

    c.bench_function("memtable_put", |b| {
        b.iter(|| {
            // Measure raw memory insertion speed
            mem.put("key".to_string(), val.clone());
        })
    });
}

criterion_group!(benches, memtable_bench);
criterion_main!(benches);
