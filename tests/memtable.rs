use gpdb::MemTable;
use std::sync::Arc;

#[test]
fn new_memtable() {
    let memtable: MemTable<String, String> = MemTable::new();
    assert_eq!(memtable.len(), 0);
}

#[test]
fn put_get() {
    let memtable: MemTable<String, String> = MemTable::new();

    let key = "key".to_string();
    let arc_value = Arc::new("value".to_string());
    memtable.put(Arc::new(key.clone()), arc_value.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(
        memtable.get(&Arc::new(key)),
        Some(Arc::new("value".to_string()))
    );
}

#[test]
fn put_overwrite() {
    let memtable: MemTable<String, String> = MemTable::new();

    let key = "key".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(Arc::new(key.clone()), arc_value1.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(
        memtable.get(&Arc::new(key.clone())),
        Some(Arc::new("value1".to_string()))
    );

    memtable.put(Arc::new(key.clone()), arc_value2.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(
        memtable.get(&Arc::new(key)),
        Some(Arc::new("value2".to_string()))
    );
}

#[test]
fn delete_value() {
    let memtable: MemTable<String, String> = MemTable::new();

    let key = "key".to_string();
    let arc_value = Arc::new("value".to_string());
    memtable.put(Arc::new(key.clone()), arc_value.clone());
    assert_eq!(memtable.len(), 1);
    assert_eq!(
        memtable.get(&Arc::new(key.clone())),
        Some(Arc::new("value".to_string()))
    );

    memtable.delete(Arc::new(key.clone()));
    assert_eq!(memtable.len(), 0);
    assert!(memtable.get(&Arc::new(key)).is_none());
}

#[test]
fn clear_all() {
    let memtable: MemTable<String, String> = MemTable::new();

    let key1 = "key1".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    memtable.put(Arc::new(key1.clone()), arc_value1.clone());

    let key2 = "key2".to_string();
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(Arc::new(key2.clone()), arc_value2.clone());

    memtable.clear();
    assert_eq!(memtable.len(), 0);
    assert!(memtable.get(&Arc::new(key1)).is_none());
    assert!(memtable.get(&Arc::new(key2)).is_none());
}

#[test]
fn iter() {
    let memtable: MemTable<String, String> = MemTable::new();

    let key1 = "key1".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    memtable.put(Arc::new(key1.clone()), arc_value1.clone());

    let key2 = "key2".to_string();
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(Arc::new(key2.clone()), arc_value2.clone());

    let mut iter = memtable.iter();

    let (k, entry) = iter.next().expect("Iterator should have at least 1 item");
    assert_eq!(k, Arc::new(key1));
    assert!(!entry.is_tombstone);
    let v1 = entry.value.as_ref().unwrap();
    assert_eq!(v1.as_str(), "value1");

    let (k, entry) = iter.next().expect("Iterator should have at least 2 items");
    assert_eq!(k, Arc::new(key2));
    assert!(!entry.is_tombstone);
    let v2 = entry.value.as_ref().unwrap();
    assert_eq!(v2.as_str(), "value2");
    assert!(iter.next().is_none());
}

#[test]
fn iter_tombstone() {
    let memtable: MemTable<String, String> = MemTable::new();

    let key1 = "key1".to_string();
    let arc_value1 = Arc::new("value1".to_string());
    memtable.put(Arc::new(key1.clone()), arc_value1.clone());

    let key2 = "key2".to_string();
    let arc_value2 = Arc::new("value2".to_string());
    memtable.put(Arc::new(key2.clone()), arc_value2.clone());

    memtable.delete(Arc::new(key1.clone()));

    let mut iter = memtable.iter();

    let (k, entry) = iter.next().expect("Iterator should have at least 2 item");
    assert_eq!(k, Arc::new(key1));
    assert!(entry.is_tombstone);
    assert!(entry.value.is_none());

    let (k, entry) = iter.next().expect("Iterator should have at least 2 item");
    assert_eq!(k, Arc::new(key2));
    assert!(!entry.is_tombstone);
    assert_eq!(entry.value.as_ref().unwrap(), &arc_value2);

    assert!(iter.next().is_none());
}
