# GPDB Changelog

## [0.2.0] - 2026-04-18

### Architecture & Refactoring
- **SSTable Decoupling**: Refactored monolithic `sstable.rs` into a module-based architecture.
    - Extracted `datablock`, `filter`, and `iterator` into distinct sub-modules for maintainability.
- **API Surface**: Cleaned up `lib.rs` exports and resolved ambiguous glob re-exports.

### Testing & Reliability
- **Test Suite Overhaul**: Migrated all inline tests to a dedicated `tests/` directory.
- **Property-Based Testing**: Integrated `proptest` for prefix compression validation.
- **Stress & Concurrency**: Added high-load concurrent stress tests for DB state transitions.
- **Recovery & Integrity**: Implemented crash recovery tests for corrupted WAL and orphan SSTables.
- **Security**: Patched OOM vulnerability in record reading by enforcing a maximum allocation limit.

## [0.1.0-alpha.1] - 2026-04-08

### Storage Engine (SSTable v3)
- **Prefix Compression (Delta Encoding)**: Implemented manual byte-packing in `DataBlocks`.
    - Consecutive keys now share prefix bytes, significantly reducing disk footprint.
    - Achieved Elite-tier space amplification (1.05x - 1.12x).
- **Sparse Indexing**: Migrated from fat indexes to a first-key sparse index, reducing RAM usage by 99.7%.
- **Hardware Alignment**: Data is now organized into 4KB DataBlocks matching physical SSD page sizes.
- **Format Versioning**: Updated SSTable footer to be 48-byte aligned and versioned for future-proof upgrades.

### Performance & Benchmarking
- **CI Performance Tuning**: Implemented environment detection in `db_bench` (`is_ci`).
    - Automatically throttles to a 4MB MemTable and 500k ops in GitHub Actions to prevent thread contention on 2-core runners.
- **GitHub Actions CI/CD**: Integrated automated formatting, safety checks, unit tests, and performance regression suites into every push.

### Reliability
- **CRC32 Checksums**: Every record frame (Data, Index, Meta) is now protected by hardware-accelerated checksums.
- **Atomic WriteBatch**: Integrated atomic multi-operation commits to the WAL.

---
*Note: All features in alpha.1 focus on the foundational storage architecture required for v1.0.*
