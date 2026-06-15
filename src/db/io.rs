//! Low-level I/O utilities.
//!
//! Provides helpers for reading and writing structured records and variable-length integers.

use crate::{Error, Result};
use crc32fast::Hasher;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};
use std::sync::Arc;

/// Writes a serialized record with a checksum and length prefix to the writer.
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

/// Reads a serialized record and verifies its checksum from the reader.
pub fn read_record<R: Read, T: DeserializeOwned>(reader: &mut R) -> Result<Option<T>> {
    let mut checksum_bytes = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut checksum_bytes) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(Error::Io(Arc::new(e)));
    }
    let expected_checksum = u32::from_le_bytes(checksum_bytes);

    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corruption("Unexpected EOF while reading record length".to_string())
        } else {
            Error::Io(Arc::new(e))
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
            Error::Io(Arc::new(e))
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

/// Writes a 64-bit integer using variable-length encoding to the writer.
pub fn write_varint<W: Write>(writer: &mut W, mut value: u64) -> Result<u64> {
    let mut bytes_written = 0;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
            writer.write_all(&[byte])?;
            bytes_written += 1;
        } else {
            writer.write_all(&[byte])?;
            bytes_written += 1;
            break;
        }
    }
    Ok(bytes_written)
}

/// Reads a 64-bit integer using variable-length encoding from the reader.
pub fn read_varint<R: Read>(reader: &mut R) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::Corruption("Varint overflow".to_string()));
        }
    }
    Ok(value)
}
