# StreamDb

[![License: LGPLv3](https://img.shields.io/badge/License-LGPLv3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Rust Version](https://img.shields.io/badge/Rust-1.75%2B-orange)](https://www.rust-lang.org/)
![Static Badge](https://img.shields.io/badge/C-embedded-green)
[![C Standard](https://img.shields.io/badge/C-C11-blue.svg)](https://en.wikipedia.org/wiki/C11_(C_standard_revision))

![](./StreamDB-logo.svg)

StreamDb is a lightweight, embedded key-value store optimized for storing and retrieving binary streams (blobs) associated with string/binary paths/keys. It uses a **reverse trie** (suffix trie) index to enable efficient **suffix-based searches** — ideal for file-extension lookups, domain patterns, asset paths in game engines, IoT telemetry, messaging frameworks, and similar low-latency workloads.

Two independent implementations exist under the same project umbrella:

- **Rust version** — full-featured, high-performance edition with WAL, compression, async I/O, streaming reads, LRU caching, memory-mapped files, WASM support, and strong FFI bindings  
- **C version** — minimal, highly portable sibling focused on embeddability in constrained/legacy environments (microcontrollers, no_std-like footprints, no Rust dependency)

Both draw inspiration from the same clean-room design concepts (originally explored in Iain Ballard’s public BSD-licensed C# prototype), but are independently written with no reverse engineering of any proprietary format. The project is licensed under **LGPLv3** (Rust) / **LGPLv2.1+** (C) to support broad FOSS and commercial adoption.

## Key Features

| Feature                          | Rust Edition                          | C Edition                           |
|----------------------------------|---------------------------------------|-------------------------------------|
| **Primary index**                | Reverse Trie (`im::OrdMap`)           | Reverse Trie (array[256] children)  |
| **Suffix search**                | Yes — O(k + m)                        | Yes — O(k + m)                      |
| **Max value size**               | 256 MB (configurable)                 | ~2 GB (platform `size_t` limited)   |
| **Thread safety**                | Multiple readers, serialized writers  | Recursive mutex (all serialized)    |
| **Persistence**                  | Write-ahead logging (`okaywal`)       | Atomic temp-file + rename           |
| **Compression**                  | Snappy (optional)                     | Not yet (planned)                   |
| **Async / Streaming**            | `get_async`, `get_stream`             | No (planned for v3)                 |
| **Caching**                      | LRU + prefetching                     | No (simple hot-path cache possible) |
| **Quick mode** (skip CRC)        | Yes (~10× faster reads)               | No                                  |
| **WASM / no_std support**        | Yes (with fallback backend)           | Native (very small footprint)       |
| **FFI bindings**                 | Comprehensive C API                   | Native C API                        |
| **Binary size (release)**        | ~few MB (with deps)                   | ~10–50 KB                           |
| **Dependencies**                 | Moderate (im, okaywal, snappy, …)     | None (pure C11 + pthreads)          |

## When to choose which version?

- Use **Rust** if you want: maximum performance, modern features (async, compression, WAL durability), WASM/browser support, or you're already in a Rust project (Bevy, DCF, Tokio-based systems).
- Use **C** if you need: minimal footprint, no external dependencies, easy integration into legacy C/C++ codebases, or deployment on deeply embedded platforms without a Rust toolchain.

## Quick Start – Rust

```toml
# Cargo.toml
[dependencies]
streamdb = "0.1"   # once published
```

```rust
use streamdb::{Config, StreamDb};

fn main() -> Result<(), streamdb::StreamDbError> {
    let mut db = StreamDb::open("assets.db", Config::default())?;
    db.set_quick_mode(true);  // optional: ~100 MB/s reads in trusted env

    // Write binary stream
    db.write_document("/textures/player.png", &mut Cursor::new(...))?;

    // Read back
    let data = db.get("/textures/player.png")?;
    
    // Suffix search: all .png files
    let pngs = db.search_by_suffix(".png")?;
    
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

- Complete WAL checkpointing & recovery testing
- Full WASM integration tests
- Python & C# bindings via FFI

**C-specific:**

- Optional compression (miniz / lz4)
- Read-write lock for better read concurrency
- Memory-mapped I/O mode

## License

- Rust implementation → **GNU Lesser General Public License v3.0**
- C implementation   → **GNU Lesser General Public License v2.1 or later**

Copyright © 2025 DeMoD LLC

Contributions are welcome — fork, branch, test, PR.

Happy streaming!
