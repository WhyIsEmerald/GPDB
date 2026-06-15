//! GPDB: A high-performance LSM-Tree storage engine.
//!
//! This crate provides the core functionality for a log-structured merge-tree
//! key-value store, including memory tables, SSTables, and a write-ahead log.

/// Database modules.
pub mod db;
/// Type definitions and traits.
pub mod types;

pub use db::compaction::stream::*;
pub use db::compaction::*;
pub use db::sstable::datablock::*;
pub use db::*;
pub use types::{
    batch::*, records::*, result::*, sstable::COMPRESSION_LZ4, sstable::COMPRESSION_NONE,
    sstable::COMPRESSION_ZSTD, sstable::FILTER_TYPE_XOR8, sstable::FILTER_TYPE_XOR16,
    sstable::SSTableId, sstable::TableMeta,
};
