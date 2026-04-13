use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Unique identifier for an SSTable.
pub struct SSTableId(pub u64);

impl std::fmt::Display for SSTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Pad to 20 digits
        write!(f, "{:020}", self.0)
    }
}

pub const FILTER_TYPE_XOR8: u8 = 0;
pub const FILTER_TYPE_XOR16: u8 = 1;

pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_ZSTD: u8 = 1; // For future alpha releases

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Metadata for an SSTable, stored in the file.
pub struct TableMeta<K> {
    pub min_key: K,
    pub max_key: K,
    pub num_entries: u64,
    pub filter_type: u8,
    pub compression_type: u8,
}
