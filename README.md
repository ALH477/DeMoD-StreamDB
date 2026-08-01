# StreamDb

[![License: LGPLv3](https://img.shields.io/badge/License-LGPLv3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Rust Version](https://img.shields.io/badge/Rust-1.75%2B-orange)](https://www.rust-lang.org/)
![Static Badge](https://img.shields.io/badge/C-embedded-green)
[![C Standard](https://img.shields.io/badge/C-C11-blue.svg)](https://en.wikipedia.org/wiki/C11_(C_standard_revision))

![](./StreamDB-logo.svg)

StreamDb is a lightweight, embedded key-value store optimized for storing and retrieving binary streams (blobs) associated with string/binary paths/keys. It uses a **reverse trie** (suffix trie) index to enable efficient **suffix-based searches** — ideal for file-extension lookups, domain patterns, asset paths in game engines, IoT telemetry, messaging frameworks, and similar low-latency workloads.

Two **independent** implementations exist under the same project umbrella:

- **Rust version** (`Rust/`) — the reference implementation: crash-safe append-only persistence (v3 format), LRU caching, memory-mapped reads, checksums, compaction, C FFI bindings
- **C version** (`C/`) — minimal, highly portable sibling for constrained/legacy environments, pure C11 with zero dependencies and a background auto-flush thread

> **⚠ The two on-disk formats are incompatible.** The Rust v3 format (dual
> CRC'd header slots, append-only, per-document CRC32) is the reference design;
> the C format is a native-endian recursive trie dump without checksums or
> fsync. Files cannot be exchanged between the implementations. See
> `Rust/README.md` and `C/README.md` for the respective durability contracts.

Both draw inspiration from the same clean-room design concepts (originally explored in Iain Ballard’s public BSD-licensed C# prototype), but are independently written with no reverse engineering of any proprietary format. The project is licensed under **LGPLv3** (Rust) / **LGPLv2.1+** (C) to support broad FOSS and commercial adoption.

## Key Features

| Feature                          | Rust Edition                                   | C Edition                           |
|----------------------------------|------------------------------------------------|-------------------------------------|
| **Primary index**                | Reverse Trie (`im::OrdMap`)                    | Reverse Trie (array[256] children)  |
| **Suffix search**                | Yes — O(k + m), plus bounded variant           | Yes — O(k + m)                      |
| **Max value size**               | 256 MB                                         | ~2 GB (platform `size_t` limited)   |
| **Thread safety**                | Multiple readers, serialized writers           | Recursive mutex (all serialized)    |
| **Persistence**                  | Append-only v3, dual CRC'd commit slots        | Whole-DB temp-file + rename         |
| **Crash recovery**               | Torn-write fallback to previous commit         | No (no fsync, no checksums)         |
| **Space reclamation**            | `compact()`                                    | No                                  |
| **Double-open guard**            | Exclusive advisory file lock                   | No                                  |
| **Auto-flush**                   | No (call `flush()` explicitly)                 | Yes (background thread)             |
| **Caching**                      | LRU                                            | No                                  |
| **Checksum on read**             | CRC32 per document (can be disabled)           | No                                  |
| **WASM / no_std support**        | wasm32 target (no mmap/file-lock)              | Native (very small footprint)       |
| **FFI bindings**                 | Comprehensive C API (`ffi` feature)            | Native C API                        |
| **Binary size (release)**        | ~few MB (with deps)                            | ~10–50 KB                           |
| **Dependencies**                 | Moderate (im, parking_lot, lru, …)             | None (pure C11 + pthreads)          |

## When to choose which version?

- Use **Rust** if you want: crash safety, data-integrity checksums, space reclamation, a guaranteed on-disk format, or you're already in a Rust project.
- Use **C** if you need: minimal footprint, no external dependencies, background auto-flush, easy integration into legacy C/C++ codebases, or deployment on deeply embedded platforms without a Rust toolchain — and can accept best-effort persistence.

## Quick Start – Rust

```toml
# Cargo.toml
[dependencies]
streamdb = "2.0"
```

```rust
use streamdb::{Config, Result, StreamDb};

fn main() -> Result<()> {
    let db = StreamDb::open("assets.db", Config::default())?;

    // Write binary stream
    db.insert(b"/textures/player.png", &[0x89, 0x50, 0x4E, 0x47])?;

    // Read back
    let data = db.get(b"/textures/player.png")?;

    // Suffix search: all .png files
    let pngs = db.suffix_search(b".png")?;

    db.flush()?;      // durable commit
    db.compact()?;    // reclaim space from deletes/superseded commits
    Ok(())
}
```

## Quick Start – C

```c
#include <streamdb.h>

int main(void) {
    StreamDB *db = streamdb_init("mydb.dat", 5000);  // 5s auto-flush

    streamdb_insert(db, (const unsigned char*)"user:alice", 10,
                    "Alice Smith", 11);

    size_t len;
    char *value = streamdb_get(db, (const unsigned char*)"user:alice", 10, &len);
    if (value) {
        printf("Found: %.*s\n", (int)len, value);
        free(value);
    }

    // Find everything ending with "alice"
    StreamDBResult *results = streamdb_suffix_search(db, (const unsigned char*)"alice", 5);
    // ... iterate results ...

    streamdb_free_results(results);
    streamdb_free(db);  // flushes & cleans up
    return 0;
}
```

Compile:
```bash
gcc -o example example.c -lstreamdb -lpthread -O2
```

## Status & Roadmap

Both implementations are functional and production-viable for many embedded/real-time use cases, but are still evolving toward full maturity.

**Near-term priorities (shared):**

- Trie path compression (lower memory footprint)
- Better suffix/prefix duality (optional forward index)
- Performance regression suite in CI
- Extended documentation & usage examples

**Rust-specific:**

- Optional deterministic document IDs (UUID v5) for reproducible files
- Full WASM integration tests
- Python & C# bindings via FFI

**C-specific:**

- Port the v3 commit protocol (checksums, fsync, dual header slots)
- Optional compression (miniz / lz4)
- Read-write lock for better read concurrency
- Memory-mapped I/O mode

## License

- Rust implementation → **GNU Lesser General Public License v3.0**
- C implementation   → **GNU Lesser General Public License v2.1 or later**

Copyright © 2025 DeMoD LLC

Contributions are welcome — fork, branch, test, PR.

Happy streaming!
