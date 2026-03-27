# GPDB

GPDB is a high-performance, embedded key-value store built on a Log-Structured Merge-Tree (LSM-Tree) architecture. Designed for speed and reliability, GPDB features a unique dual-structure MemTable and an asynchronous background compaction coordinator.

## Key Features

*   **High-Performance LSM-Tree:** Robust storage engine with multi-level automated compaction.
*   **Asynchronous Coordination:** Compaction work is offloaded to a dedicated background thread to ensure non-blocking writes.
*   **O(1) Memory Layer:** A parallel HashMap/BTreeMap design eliminates the O(log N) bottleneck for recently written data.
*   **Data Integrity:** Every record and index block is protected by CRC32 checksums and length framing to detect disk corruption.
*   **Durable Recovery:** An append-only Write-Ahead Log (WAL) and atomic Manifest system ensure state consistency across crashes.

## Tech Stack

*   **Language:** Rust (2024 Edition)
*   **Serialization:** `bincode` for efficient binary storage
*   **Error Handling:** `thiserror` for descriptive, database-specific errors
*   **I/O:** Buffered disk I/O with checksum validation

## Getting Started

GPDB is a library designed to be embedded in your Rust applications.

### Basic Usage

```rust
use gpdb::DB;

// Open a database (or create one) with a 10MB MemTable limit
let mut db = DB::open(Path::new("./data"), 10 * 1024 * 1024)?;

// Put and Get
db.put("user_123".to_string(), "John Doe".to_string())?;
if let Some(val) = db.get(&"user_123".to_string())? {
    println!("Found: {}", val);
}

// Delete
db.delete("user_123".to_string())?;
```

## Project Status: Phase 1 Complete

GPDB has successfully completed **Phase 1: Core Storage Engine**.

*   [x] **Durable Writes**: WAL and MemTable integration.
*   [x] **Persistent Storage**: SSTable format with metadata and checksums.
*   [x] **Metadata Management**: Atomic Manifest log for state recovery.
*   [x] **Automated Maintenance**: Multi-level background compaction coordinator.

We are now entering **Phase 2: Production-Readiness & Performance**, focusing on concurrency, caching, and advanced API features.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for our workflow, branch naming standards, and coding guidelines.

## License

This project is licensed under the MIT License.

## Contact

[WhyIsEmerald](https://github.com/WhyIsEmerald) - WhyIsEmerald@proton.me
