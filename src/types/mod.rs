//! Core data types for GPDB.
//!
//! This module defines the fundamental records, result types, and traits
//! used throughout the storage engine.

/// Record and entry definitions.
pub mod records;
pub use records::*;

/// Memory footprint calculation traits.
pub mod sizable;
pub use sizable::*;

/// Error and result type definitions.
pub mod result;
pub use result::*;

/// SSTable identity and metadata definitions.
pub mod sstable;
pub use sstable::*;

/// Atomic write batch definitions.
pub mod batch;
pub use batch::*;
