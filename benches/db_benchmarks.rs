use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gpdb::{DB, Entry, LogEntry, Manifest, ManifestEntry, MemTable, ValueEntry, Wal};
use std::io::Cursor;
use std::sync::Arc;
use tempfile::TempDir;

pub fn memtable_bench(c: &mut Criterion) {
    let mut mem = MemTable::new();
    let val = Arc::new("value-000000000000000000000000000000".to_string());

    c.bench_function("memtable_put_30b", |b| {
        b.iter(|| {
            mem.put(
                black_box("key-0000000000".to_string()),
                black_box(val.clone()),
            );
        })
    });
}

pub fn io_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_primitives");
    let entry = Entry {
        key: "key-0000000000".to_string(),
        value: ValueEntry {
            value: Some(Arc::new("value-00000000000000000000".to_string())),
            is_tombstone: false,
        },
    };

    group.bench_function("write_record_50b", |b| {
        let mut buf = Vec::with_capacity(1024);
        b.iter(|| {
            buf.clear();
            gpdb::db::io::write_record(&mut buf, black_box(&entry)).unwrap();
        })
    });

    let mut encoded = Vec::new();
    gpdb::db::io::write_record(&mut encoded, &entry).unwrap();

    group.bench_function("read_record_50b", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&encoded);
            let _: Option<Entry<String, String>> = gpdb::db::io::read_record(&mut cursor).unwrap();
        })
    });
    group.finish();
}

pub fn persistence_bench(c: &mut Criterion) {
    let tmp_dir = TempDir::new().unwrap();

    // WAL Bench
    let wal_path = tmp_dir.path().join("wal.log");
    let mut wal: Wal<String, String> = Wal::create(&wal_path).unwrap();
    let entry: LogEntry<String, String> =
        LogEntry::Put("key".to_string(), Arc::new("val".to_string()));

    c.bench_function("wal_append_and_flush", |b| {
        b.iter(|| {
            wal.append(black_box(&entry)).unwrap();
            wal.flush().unwrap();
        })
    });

    // Manifest Bench
    let manifest_path = tmp_dir.path().join("MANIFEST");
    let mut manifest = Manifest::create(manifest_path).unwrap();
    let m_entry = ManifestEntry::AddSSTable {
        level: 0,
        path: "L0-1.sst".into(),
    };

    c.bench_function("manifest_append_and_flush", |b| {
        b.iter(|| {
            manifest.append(black_box(&m_entry)).unwrap();
            manifest.flush().unwrap();
        })
    });
}

pub fn db_orchestration_bench(c: &mut Criterion) {
    let tmp_dir = TempDir::new().unwrap();
    // Use huge MemTable limit to measure pure orchestration speed (avoiding flushes)
    let mut db: DB<String, String> = DB::open(tmp_dir.path(), 100 * 1024 * 1024).unwrap();

    c.bench_function("db_put_orchestration", |b| {
        b.iter(|| {
            db.put(black_box("key".to_string()), black_box("val".to_string()))
                .unwrap();
        })
    });

    c.bench_function("db_get_memtable_hit", |b| {
        b.iter(|| {
            db.get(black_box(&"key".to_string())).unwrap();
        })
    });
}

criterion_group!(
    benches,
    memtable_bench,
    io_bench,
    persistence_bench,
    db_orchestration_bench
);
criterion_main!(benches);
