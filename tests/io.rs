use gpdb::Result;
use gpdb::db::io::{read_record, write_record};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestStruct {
    id: u32,
    name: String,
    tags: Vec<String>,
}

#[test]
fn write_read_roundtrip() {
    let mut buffer = Vec::new();
    let original = TestStruct {
        id: 42,
        name: "GPDB".to_string(),
        tags: vec!["LSM".to_string(), "Rust".to_string()],
    };

    let bytes_written = write_record(&mut buffer, &original).expect("Write failed");
    assert!(bytes_written > 0);

    let mut cursor = Cursor::new(buffer);
    let recovered: TestStruct = read_record(&mut cursor)
        .expect("Read failed")
        .expect("Expected a record, got None");

    assert_eq!(original, recovered);
}

#[test]
fn read_eof() {
    let empty_buffer: Vec<u8> = Vec::new();
    let mut cursor = Cursor::new(empty_buffer);

    let result: Option<TestStruct> = read_record(&mut cursor).expect("Read failed");
    assert!(result.is_none());
}

#[test]
fn checksum_mismatch() {
    let mut buffer = Vec::new();
    let data = TestStruct {
        id: 1,
        name: "Normal".to_string(),
        tags: vec![],
    };
    write_record(&mut buffer, &data).unwrap();

    let last_idx = buffer.len() - 1;
    buffer[last_idx] ^= 0xFF;

    let mut cursor = Cursor::new(buffer);
    let result: Result<Option<TestStruct>> = read_record(&mut cursor);

    match result {
        Err(gpdb::Error::Corruption(msg)) => assert!(msg.contains("checksum mismatch")),
        _ => panic!("Expected Corruption error, got {:?}", result),
    }
}

#[test]
fn unexpected_eof_in_middle() {
    let mut buffer = Vec::new();
    let data = TestStruct {
        id: 1,
        name: "Short".to_string(),
        tags: vec![],
    };
    write_record(&mut buffer, &data).unwrap();

    buffer.truncate(buffer.len() - 5);

    let mut cursor = Cursor::new(buffer);
    let result: Result<Option<TestStruct>> = read_record(&mut cursor);

    match result {
        Err(gpdb::Error::Corruption(msg)) => assert!(msg.contains("Unexpected EOF")),
        _ => panic!("Expected Corruption error, got {:?}", result),
    }
}

#[test]
fn multiple_records() {
    let mut buffer = Vec::new();
    let r1 = TestStruct {
        id: 1,
        name: "A".to_string(),
        tags: vec![],
    };
    let r2 = TestStruct {
        id: 2,
        name: "B".to_string(),
        tags: vec![],
    };

    write_record(&mut buffer, &r1).unwrap();
    write_record(&mut buffer, &r2).unwrap();

    let mut cursor = Cursor::new(buffer);

    let rec1: TestStruct = read_record(&mut cursor).unwrap().unwrap();
    let rec2: TestStruct = read_record(&mut cursor).unwrap().unwrap();
    let rec3: Option<TestStruct> = read_record(&mut cursor).unwrap();

    assert_eq!(r1, rec1);
    assert_eq!(r2, rec2);
    assert!(rec3.is_none());
}
