# GPDB

GPDB is a high-performance, embedded key-value store built on a Log-Structured Merge-Tree (LSM-Tree) architecture. Designed for speed and reliability, GPDB features a unique dual-structure MemTable that provides O(1) read performance for recently written data, making it ideal for workloads with heavy "read-after-write" patterns.

## Project Goals

*   **High-Performance LSM-Tree:** Implement a robust storage engine with efficient SSTable management and leveled compaction.
*   **Read-Optimized Memory Layer:** Leverage a parallel HashMap/BTreeMap design to eliminate the O(log N) bottleneck for recent data lookups.
*   **Tunable Durability:** Provide a flexible Write-Ahead Log (WAL) system that allows users to balance between maximum write throughput and strict data safety.
*   **Production Readiness:** Ensure data integrity through CRC32 checksums and support concurrent access via thread-safe primitives.
*   **Developer Friendly:** Offer a clean, embedded API for Rust applications requiring a persistent, sorted key-value store.

## Tech Stack

*   **Core Logic:** Rust (for memory safety and zero-cost abstractions)
*   **Concurrency:** `Arc`, `Mutex`, and `RwLock` for thread-safe operations
*   **Serialization:** Custom binary format for SSTables and WAL records
*   **Data Integrity:** CRC32 checksums for hardware corruption protection

## Getting Started

GPDB is designed to be used as a library within Rust projects. While the project is currently in active development (Phase 1), the core storage engine components are being finalized.

### Development Workflow (Planned)

*   **Run Unit Tests:** `cargo test` (to verify MemTable, WAL, and SSTable logic)
*   **Integration Testing:** `cargo test --test integration` (for full DB lifecycle tests)
*   **Performance Benchmarking:** `cargo bench` (to measure read/write throughput)

## Contributing

Contributions are welcome! If you're an external developer looking to contribute, please refer to our [[03-projects/GPDB/CONTRIBUTING|Contribution Guide]] for details on our workflow and coding standards.

## Project Status

GPDB is currently in **Phase 1: Core Storage Engine**. 
*   **Completed:** MemTable (Dual-structure), WAL (Log-structured), SSTable (Basic Read/Write), and Leveled Structure design.
*   **In Progress:** Manifest file implementation for database metadata and the Compaction Coordinator.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

Author Name - [WhyIsEmerald]([https://twitter.com/twitter_handle](https://github.com/WhyIsEmerald)) - WhyIsEmerald@proton.me

Project Link: [https://github.com/WhyIsEmerald/GPDB](https://github.com/WhyIsEmerald/GPDB)
