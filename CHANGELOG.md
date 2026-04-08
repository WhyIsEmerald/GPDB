# GPDB Changelog

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
