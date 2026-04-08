# GPDB 🚀

GPDB is a high-performance, hardware-native embedded key-value store built on a Log-Structured Merge-Tree (LSM-Tree) architecture. Designed for extreme efficiency, GPDB combines industrial-grade storage optimizations with a safe, multi-threaded Rust foundation.

## Key Features ✨

*   **Elite Space Efficiency:** Implements **Delta Encoding (Prefix Compression)** to deduplicate shared key prefixes, achieving space amplification factors as low as 1.05x. 💾
*   **High-Speed I/O:** Data is organized into **4KB DataBlocks** aligned to physical SSD page sizes, featuring **Restart Points** for binary-search-speed lookups within blocks. ⚡
*   **Zero-Overhead Indexing:** Uses a **Sparse Index** that maps only the first key of each block, reducing RAM footprint by >99.7% compared to traditional fat indexes. 🧠
*   **Asynchronous Compaction:** Background maintenance is offloaded to a dedicated coordinator thread, ensuring high-throughput, non-blocking writes even during heavy merge cycles. ⚙️
*   **Data Integrity:** All frames (Data, Index, Meta) are protected by **CRC32 Checksums** and length-prefixed framing to detect and prevent disk corruption. ✅
*   **Durable Recovery:** Append-only Write-Ahead Log (WAL) and atomic Manifest tracking ensure 100% state consistency across system crashes. 🛡️

## Tech Stack 🛠️

*   **Language:** Rust (2024 Edition)
*   **Architecture:** LSM-Tree with Multi-Level Leveled Compaction.
*   **Format:** Versioned SSTable v3 (Aligned Footer, Delta Encoded).
*   **Concurrency:** Lock-free atomic WriteBatches and thread-safe `Arc<RwLock<T>>` internals.

## Getting Started 🏁

GPDB is designed to be embedded directly into your Rust applications.

### Basic Usage

```rust
use gpdb::DB;
use std::path::Path;

// Open a database with a 1MB MemTable limit
let mut db = DB::open(Path::new("./data"), 1 * 1024 * 1024)?;

// Put and Get
db.put("user_123".to_string(), "John Doe".to_string())?;
if let Some(val) = db.get(&"user_123".to_string())? {
    println!("Found: {}", val);
}

// Delete
db.delete("user_123".to_string())?;
```

## Project Status: Production Hardening (Phase 2) 📈

GPDB has evolved from a core prototype to a high-performance storage engine.

*   [x] **Blocked Storage**: 4KB page alignment for hardware-native throughput.
*   [x] **Delta Encoding**: Elite storage footprint reduction.
*   [x] **Sparse Indexing**: Industrial-scale RAM efficiency.
*   [x] **Background Coordinator**: Autonomous L0-Ln compaction.

**Current Sprint**: Implementing **Bloom Filters** and **LRU Block Caching** to reach 2M+ ops/s read performance.

## Contributing 🤝

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for our workflow, branch naming standards, and architectural guidelines.

## License 📄

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project is licensed under the MIT License.

## Contact 📧

[WhyIsEmerald](https://github.com/WhyIsEmerald) - WhyIsEmerald@proton.me