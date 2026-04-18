use crate::{Error, Result};
use crc32fast::Hasher;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};

/// Writes a data-frame to the writer: [Checksum (4), Length (8), Data]
/// Returns the total number of bytes written.
pub fn write_record<W: Write, T: Serialize>(writer: &mut W, data: &T) -> Result<u64> {
    let serialized_data =
        bincode::serialize(data).map_err(|e| Error::Serialization(e.to_string()))?;

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
    let mut checksum_bytes = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut checksum_bytes) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(Error::Io(e));
    }
    let expected_checksum = u32::from_le_bytes(checksum_bytes);

    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corruption("Unexpected EOF while reading record length".to_string())
        } else {
            Error::Io(e)
        }
    })?;
    let len = u64::from_le_bytes(len_bytes) as usize;

    const MAX_RECORD_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    if len > MAX_RECORD_SIZE {
        return Err(Error::Corruption(format!(
            "Record size {} exceeds maximum of {}",
            len, MAX_RECORD_SIZE
        )));
    }

    let mut data_bytes = vec![0; len];
    reader.read_exact(&mut data_bytes).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corruption("Unexpected EOF while reading record data".to_string())
        } else {
            Error::Io(e)
        }
    })?;

    let mut hasher = Hasher::new();
    hasher.update(&data_bytes);
    if hasher.finalize() != expected_checksum {
        return Err(Error::Corruption("Record checksum mismatch".to_string()));
    }

    let data: T =
        bincode::deserialize(&data_bytes).map_err(|e| Error::Serialization(e.to_string()))?;

    Ok(Some(data))
}
