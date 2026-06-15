//! SSTable type definitions.
//!
//! Defines the identifiers, metadata, and configuration constants for on-disk sorted string tables.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// A struct that represents a unique identifier for an SSTable.
pub struct SSTableId(pub u64);

impl std::fmt::Display for SSTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

/// The filter type used for XOR filters with 8-bit buckets.
pub const FILTER_TYPE_XOR8: u8 = 0;
/// The filter type used for XOR filters with 16-bit buckets.
pub const FILTER_TYPE_XOR16: u8 = 1;

/// The compression type used for no compression.
pub const COMPRESSION_NONE: u8 = 0;
/// The compression type used for Zstandard compression.
pub const COMPRESSION_ZSTD: u8 = 1;
/// The compression type used for LZ4 compression.
pub const COMPRESSION_LZ4: u8 = 2;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// A struct that represents metadata for an SSTable.
pub struct TableMeta<K> {
    /// The smallest key used in the table.
    pub min_key: K,
    /// The largest key used in the table.
    pub max_key: K,
    /// The number of entries used in the table.
    pub num_entries: u64,
    /// The filter type used for the table.
    pub filter_type: u8,
    /// The compression type used for the table.
    pub compression_type: u8,
}
