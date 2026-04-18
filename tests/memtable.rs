use gpdb::MemTable;
use std::sync::Arc;

#[test]
fn new_memtable() {
    let memtable: MemTable<String, String> = MemTable::new();
    assert_eq!(memtable.len(), 0);
}

#[test]
fn put_get() {
    let mut memtable: MemTable<String, String> = MemTable::new();

    let key = "key".to_string();
    let arc_value = Arc::new("value".to_string());
    memtable.put(key.clone(), arc_value.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(memtable.get(&key), Some(Arc::new("value".to_string())));
}

#[test]
fn put_overwrite() {
    let mut memtable: MemTable<String, String> = MemTable::new();

    let key = "key".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(key.clone(), arc_value1.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(memtable.get(&key), Some(Arc::new("value1".to_string())));

    memtable.put(key.clone(), arc_value2.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(memtable.get(&key), Some(Arc::new("value2".to_string())));
}

#[test]
fn delete_value() {
    let mut memtable: MemTable<String, String> = MemTable::new();

    let key = "key".to_string();
    let arc_value = Arc::new("value".to_string());
    memtable.put(key.clone(), arc_value.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(memtable.get(&key), Some(Arc::new("value".to_string())));

    memtable.delete(key.clone());
    assert_eq!(memtable.len(), 0);
    assert!(memtable.get(&key).is_none());
}

#[test]
fn clear_all() {
    let mut memtable: MemTable<String, String> = MemTable::new();

    let key1 = "key1".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    memtable.put(key1.clone(), arc_value1.clone());

    let key2 = "key2".to_string();
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(key2.clone(), arc_value2.clone());

    memtable.clear();
    assert_eq!(memtable.len(), 0);
    assert!(memtable.get(&key1).is_none());
    assert!(memtable.get(&key2).is_none());
}

#[test]
fn iter() {
    let mut memtable: MemTable<String, String> = MemTable::new();

    let key1 = "key1".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    memtable.put(key1.clone(), arc_value1.clone());

    let key2 = "key2".to_string();
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(key2.clone(), arc_value2.clone());

    let mut iter = memtable.iter();

    let (k, entry) = iter.next().expect("Iterator should have at least 1 item");
    assert_eq!(k, &key1);
    assert!(!entry.is_tombstone);
    let v1 = entry.value.as_ref().unwrap();
    assert_eq!(v1.as_str(), "value1");

    let (k, entry) = iter.next().expect("Iterator should have at least 2 items");
    assert_eq!(k, &key2);
    assert!(!entry.is_tombstone);
    let v2 = entry.value.as_ref().unwrap();
    assert_eq!(v2.as_str(), "value2");
    assert!(iter.next().is_none());
}

#[test]
fn iter_tombstone() {
    let mut memtable: MemTable<String, String> = MemTable::new();

    let key1 = "key1".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    memtable.put(key1.clone(), arc_value1.clone());

    let key2 = "key2".to_string();
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(key2.clone(), arc_value2.clone());

    memtable.delete(key1.clone());

    let mut iter = memtable.iter();

    let (k, entry) = iter.next().expect("Iterator should have at least 2 item");
    assert_eq!(k, &key1);
    assert!(entry.is_tombstone);
    assert!(entry.value.is_none());

    let (k, entry) = iter.next().expect("Iterator should have at least 2 item");
    assert_eq!(k, &key2);
    assert!(!entry.is_tombstone);
    assert_eq!(entry.value.as_ref().unwrap(), &arc_value2);

    assert!(iter.next().is_none());
}
