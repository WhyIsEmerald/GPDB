use crate::{Result, Error};
use crc32fast::Hasher;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};

/// Writes a data-frame to the writer: [Checksum (4), Length (8), Data]
/// Returns the total number of bytes written.
pub fn write_record<W: Write, T: Serialize>(writer: &mut W, data: &T) -> Result<u64> {
    let serialized_data = bincode::serialize(data)
        .map_err(|e| Error::Serialization(e.to_string()))?;
    
    let len = serialized_data.len() as u64;
    let mut hasher = Hasher::new();
    hasher.update(&serialized_data);
    let checksum = hasher.finalize();

    writer.write_all(&checksum.to_le_bytes())?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&serialized_data)?;

    Ok(4 + 8 + len)
}

/// Reads a data-frame from the reader.
/// Returns Ok(None) on clean EOF at the start of a record.
pub fn read_record<R: Read, T: DeserializeOwned>(reader: &mut R) -> Result<Option<T>> {
    // 1. Read Checksum
    let mut checksum_bytes = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut checksum_bytes) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(Error::Io(e));
    }
    let expected_checksum = u32::from_le_bytes(checksum_bytes);

    // 2. Read Length
    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corruption("Unexpected EOF while reading record length".to_string())
        } else {
            Error::Io(e)
        }
    })?;
    let len = u64::from_le_bytes(len_bytes) as usize;

    // 3. Read Data
    let mut data_bytes = vec![0; len];
    reader.read_exact(&mut data_bytes).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corruption("Unexpected EOF while reading record data".to_string())
        } else {
            Error::Io(e)
        }
    })?;

    // 4. Verify Checksum
    let mut hasher = Hasher::new();
    hasher.update(&data_bytes);
    if hasher.finalize() != expected_checksum {
        return Err(Error::Corruption("Record checksum mismatch".to_string()));
    }

    // 5. Deserialize
    let data: T = bincode::deserialize(&data_bytes)
        .map_err(|e| Error::Serialization(e.to_string()))?;

    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::io::Cursor;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        id: u32,
        name: String,
        tags: Vec<String>,
    }

    #[test]
    fn test_write_read_roundtrip() {
        let mut buffer = Vec::new();
        let original = TestStruct {
            id: 42,
            name: "GPDB".to_string(),
            tags: vec!["LSM".to_string(), "Rust".to_string()],
        };

        // Write
        let bytes_written = write_record(&mut buffer, &original).expect("Write failed");
        assert!(bytes_written > 0);

        // Read
        let mut cursor = Cursor::new(buffer);
        let recovered: TestStruct = read_record(&mut cursor)
            .expect("Read failed")
            .expect("Expected a record, got None");

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_read_eof() {
        let empty_buffer: Vec<u8> = Vec::new();
        let mut cursor = Cursor::new(empty_buffer);
        
        let result: Option<TestStruct> = read_record(&mut cursor).expect("Read failed");
        assert!(result.is_none());
    }

    #[test]
    fn test_checksum_mismatch() {
        let mut buffer = Vec::new();
        let data = TestStruct {
            id: 1,
            name: "Normal".to_string(),
            tags: vec![],
        };
        write_record(&mut buffer, &data).unwrap();

        // Corrupt the data (last byte)
        let last_idx = buffer.len() - 1;
        buffer[last_idx] ^= 0xFF;

        let mut cursor = Cursor::new(buffer);
        let result: Result<Option<TestStruct>> = read_record(&mut cursor);

        match result {
            Err(Error::Corruption(msg)) => assert!(msg.contains("checksum mismatch")),
            _ => panic!("Expected Corruption error, got {:?}", result),
        }
    }

    #[test]
    fn test_unexpected_eof_in_middle() {
        let mut buffer = Vec::new();
        let data = TestStruct { id: 1, name: "Short".to_string(), tags: vec![] };
        write_record(&mut buffer, &data).unwrap();

        // Truncate the buffer to simulate a crash mid-write
        buffer.truncate(buffer.len() - 5);

        let mut cursor = Cursor::new(buffer);
        let result: Result<Option<TestStruct>> = read_record(&mut cursor);

        match result {
            Err(Error::Corruption(msg)) => assert!(msg.contains("Unexpected EOF")),
            _ => panic!("Expected Corruption error, got {:?}", result),
        }
    }

    #[test]
    fn test_multiple_records() {
        let mut buffer = Vec::new();
        let r1 = TestStruct { id: 1, name: "A".to_string(), tags: vec![] };
        let r2 = TestStruct { id: 2, name: "B".to_string(), tags: vec![] };

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
}
