use gpdb::{Compactor, Entry, MemTable, MergeElement, MergeStream, SSTable, SSTableId, ValueEntry};
use std::sync::Arc;
use tempfile::TempDir;

fn make_el(key: &str, id: u64, iter_idx: usize) -> MergeElement<String, String> {
    MergeElement {
        sstable_id: SSTableId(id),
        entry: Entry {
            key: key.to_string(),
            value: ValueEntry {
                value: Some(Arc::new("val".to_string())),
                is_tombstone: false,
            },
        },
        iter_index: iter_idx,
    }
}

#[test]
fn key_priority() {
    use std::collections::BinaryHeap;
    let mut heap = BinaryHeap::new();

    heap.push(make_el("C", 1, 0));
    heap.push(make_el("A", 1, 0));
    heap.push(make_el("B", 1, 0));

    assert_eq!(heap.pop().unwrap().entry.key, "A");
    assert_eq!(heap.pop().unwrap().entry.key, "B");
    assert_eq!(heap.pop().unwrap().entry.key, "C");
}

#[test]
fn id_priority_on_tie() {
    use std::collections::BinaryHeap;
    let mut heap = BinaryHeap::new();

    heap.push(make_el("A", 10, 0));
    heap.push(make_el("A", 2, 0));
    heap.push(make_el("A", 5, 0));

    let winner = heap.pop().unwrap();
    assert_eq!(winner.entry.key, "A");
    assert_eq!(winner.sstable_id, SSTableId(10));

    assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(5));
    assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(2));
}

#[test]
fn complex_mix() {
    use std::collections::BinaryHeap;
    let mut heap = BinaryHeap::new();

    heap.push(make_el("B", 100, 0));
    heap.push(make_el("A", 1, 0));
    heap.push(make_el("B", 50, 0));
    heap.push(make_el("A", 50, 0));

    let first = heap.pop().unwrap();
    assert_eq!(first.entry.key, "A");
    assert_eq!(first.sstable_id, SSTableId(50));

    assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(1));

    assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(100));
    assert_eq!(heap.pop().unwrap().sstable_id, SSTableId(50));
}

#[test]
fn merge_stream_integration() {
    let tmp_dir = TempDir::new().unwrap();

    let sst1_path = tmp_dir.path().join("L0-1.sst");
    let mut mem1 = MemTable::new();
    mem1.put("A".to_string(), Arc::new("v1-old".to_string()));
    mem1.put("C".to_string(), Arc::new("v1".to_string()));
    mem1.put("E".to_string(), Arc::new("v1".to_string()));
    let sst1 = SSTable::write_from_memtable(&sst1_path, &mem1, SSTableId(1), None).unwrap();

    let sst2_path = tmp_dir.path().join("L0-2.sst");
    let mut mem2 = MemTable::new();
    mem2.put("A".to_string(), Arc::new("v2-new".to_string()));
    mem2.put("B".to_string(), Arc::new("v2".to_string()));
    mem2.put("D".to_string(), Arc::new("v2".to_string()));
    let sst2 = SSTable::write_from_memtable(&sst2_path, &mem2, SSTableId(2), None).unwrap();

    let sstables = vec![sst1, sst2];
    let stream = MergeStream::new(&sstables).unwrap();

    let results: Vec<Entry<String, String>> = stream.map(|r| r.unwrap()).collect();

    assert_eq!(results[0].key, "A");
    assert_eq!(results[0].value.value.as_ref().unwrap().as_str(), "v2-new");

    assert_eq!(results[1].key, "B");
    assert_eq!(results[2].key, "C");
    assert_eq!(results[3].key, "D");
    assert_eq!(results[4].key, "E");
    assert_eq!(results.len(), 5);
}

#[test]
fn compactor_l0_to_disk() {
    let tmp_dir = TempDir::new().unwrap();

    let path1 = tmp_dir.path().join("L0-1.sst");
    let mut mem1 = MemTable::new();
    mem1.put("A".to_string(), Arc::new("old".to_string()));
    mem1.put("C".to_string(), Arc::new("val".to_string()));
    let sst1 = SSTable::write_from_memtable(&path1, &mem1, SSTableId(1), None).unwrap();

    let path2 = tmp_dir.path().join("L0-2.sst");
    let mut mem2 = MemTable::new();
    mem2.put("A".to_string(), Arc::new("new".to_string()));
    mem2.put("B".to_string(), Arc::new("val".to_string()));
    let sst2 = SSTable::write_from_memtable(&path2, &mem2, SSTableId(2), None).unwrap();

    let l1_path = tmp_dir.path().join("L1-5.sst");
    let sst_l1 =
        Compactor::compact_l0(&[sst1, sst2], &l1_path, SSTableId(5), None).expect("Compaction failed");

    assert_eq!(sst_l1.id(), SSTableId(5));
    assert_eq!(sst_l1.len(), 3);

    let val_a = sst_l1.get(&"A".to_string()).unwrap().unwrap();
    assert_eq!(val_a.value.unwrap().as_str(), "new");

    let val_b = sst_l1.get(&"B".to_string()).unwrap().unwrap();
    assert!(val_b.value.is_some());

    let val_c = sst_l1.get(&"C".to_string()).unwrap().unwrap();
    assert!(val_c.value.is_some());
}
