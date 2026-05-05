use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gpdb::DB;
use std::thread;
use tempfile::TempDir;

pub fn orchestration_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchestration");
    group.sample_size(1000);

    let tmp_dir = TempDir::new().unwrap();
    let db: DB<String, String> = DB::open(tmp_dir.path(), 100 * 1024 * 1024).unwrap();

    group.bench_function("put_single_threaded", |b| {
        b.iter(|| {
            db.put(black_box("key".to_string()), black_box("val".to_string()))
                .unwrap();
        })
    });

    group.bench_function("get_memtable_hit", |b| {
        db.put("key-target".to_string(), "val".to_string()).unwrap();
        b.iter(|| {
            db.get(black_box(&"key-target".to_string())).unwrap();
        })
    });

    group.bench_function("put_multi_threaded_contention_4_threads", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let db_clone = db.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..10 {
                        db_clone
                            .put(format!("key-{}", i), "val".to_string())
                            .unwrap();
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        })
    });

    for batch_size in [10, 100, 1000] {
        group.bench_function(format!("write_batch_{}", batch_size), |b| {
            b.iter(|| {
                let mut batch = gpdb::WriteBatch::new();
                for i in 0..batch_size {
                    batch.put(format!("key-{}", i), "val".to_string());
                }
                db.write_batch(black_box(batch)).unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, orchestration_bench);
criterion_main!(benches);
