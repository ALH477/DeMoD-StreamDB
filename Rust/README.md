# StreamDB (Rust)

A lightweight, thread-safe embedded key-value database implemented in Rust, using a reverse trie data structure for efficient suffix-based searches.

[![Crates.io](https://img.shields.io/crates/v/streamdb.svg)](https://crates.io/crates/streamdb)
[![Documentation](https://docs.rs/streamdb/badge.svg)](https://docs.rs/streamdb)
[![License: LGPL v2.1](https://img.shields.io/badge/License-LGPL_v2.1-blue.svg)](https://www.gnu.org/licenses/lgpl-2.1)

## Features

| Feature | Description |
|---------|-------------|
| **Thread-safe** | Interior mutability with proper locking (no unsafe mutable aliasing) |
| **Suffix search** | O(m + k) lookup for keys ending with a given suffix |
| **Persistent Trie** | Structural sharing via `im::OrdMap` for memory-efficient updates |
| **Binary keys** | Supports arbitrary byte sequences, not just UTF-8 strings |
| **Zero-copy reads** | Memory-mapped I/O when available |
| **C FFI** | Safe C bindings for cross-language integration |
| **Optional features** | Compression, encryption, async API |

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
streamdb = "2.0"
```

### Basic Usage

```rust
use streamdb::{StreamDb, Result};

fn main() -> Result<()> {
    // Create an in-memory database
    let db = StreamDb::open_memory()?;
    
    // Insert key-value pairs
    db.insert(b"user:alice", b"Alice Smith")?;
    db.insert(b"user:bob", b"Bob Jones")?;
    db.insert(b"admin:alice", b"Alice Admin")?;
    
    // Retrieve by key
    let value = db.get(b"user:alice")?;
    assert_eq!(value, Some(b"Alice Smith".to_vec()));
    
    // Suffix search - find all keys ending with "alice"
    let results = db.suffix_search(b"alice")?;
    assert_eq!(results.len(), 2);  // user:alice, admin:alice
    
    // Delete
    db.delete(b"user:bob")?;
    
    Ok(())
}
```

### With File Persistence

```rust
use streamdb::{StreamDb, Config, Result};

fn main() -> Result<()> {
    let db = StreamDb::open("mydb.dat", Config::default())?;
    
    db.insert(b"key", b"value")?;
    db.flush()?;  // Ensure durability
    
    Ok(())
}
```

## Feature Flags

| Flag | Description | Default |
|------|-------------|---------|
| `std` | Enable standard library | ✓ |
| `persistence` | File-based storage | ✓ |
| `compression` | LZ4 value compression | ✗ |
| `encryption` | AES-256-GCM encryption | ✗ |
| `async` | Tokio async API | ✗ |
| `ffi` | C FFI bindings | ✗ |

Enable features in `Cargo.toml`:

```toml
[dependencies]
streamdb = { version = "2.0", features = ["compression", "encryption"] }
```

## Design

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        StreamDb                             │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐    │
│  │  Public API │  │  LRU Cache   │  │  Atomic Flags    │    │
│  │   (&self)   │  │ (Mutex<LRU>) │  │  (dirty, stats)  │    │
│  └──────┬──────┘  └──────────────┘  └──────────────────┘    │
│         │                                                    │
│  ┌──────┴──────────────────────────────────────────────┐    │
│  │              Core Components                         │    │
│  │  ┌────────────────┐  ┌──────────────────────────┐   │    │
│  │  │  Trie          │  │  Backend (trait)         │   │    │
│  │  │  (RwLock<Trie>)│  │  (Arc<dyn Backend>)      │   │    │
│  │  │                │  │  ├── MemoryBackend       │   │    │
│  │  │  im::OrdMap    │  │  └── FileBackend         │   │    │
│  │  └────────────────┘  └──────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Persistent Trie with Structural Sharing

The trie uses `im::OrdMap` for O(log n) persistent updates:

```rust
// Each insert returns a new trie, sharing unchanged nodes
let trie1 = Trie::new().insert(b"hello", id1);
let trie2 = trie1.insert(b"world", id2);

// trie1 still has only "hello"
assert!(trie1.get(b"world").is_none());

// trie2 has both, but shares the "hello" path with trie1
assert!(trie2.get(b"hello").is_some());
assert!(trie2.get(b"world").is_some());
```

### Thread Safety

All operations use `&self` with interior mutability:

```rust
// Safe to share across threads
let db = Arc::new(StreamDb::open_memory()?);

// Concurrent reads
let handles: Vec<_> = (0..4).map(|_| {
    let db = Arc::clone(&db);
    thread::spawn(move || {
        db.get(b"key")
    })
}).collect();

// Concurrent writes (internally serialized)
let db_clone = Arc::clone(&db);
thread::spawn(move || {
    db_clone.insert(b"new_key", b"value")
});
```

### Lock Ordering

To prevent deadlocks, locks are always acquired in this order:

1. `trie` (RwLock) — for index operations
2. `cache` (Mutex) — for LRU cache updates
3. `backend.documents` — for storage operations

## C FFI

Build as a C library:

```bash
cargo build --release --features ffi
```

### C Usage

```c
#include "streamdb.h"

int main() {
    StreamDbHandle* db = NULL;
    
    // Open database
    if (streamdb_open_memory(&db) != STREAMDB_OK) {
        return 1;
    }
    
    // Insert
    StreamDbUuid id;
    streamdb_insert(db, (uint8_t*)"key", 3, (uint8_t*)"value", 5, &id);
    
    // Get
    StreamDbBuffer buffer = {0};
    if (streamdb_get(db, (uint8_t*)"key", 3, &buffer) == STREAMDB_OK) {
        // Use buffer.data, buffer.len
        streamdb_free_buffer(&buffer);
    }
    
    // Suffix search
    StreamDbSearchResults results = {0};
    if (streamdb_suffix_search(db, (uint8_t*)"ey", 2, &results) == STREAMDB_OK) {
        for (size_t i = 0; i < results.count; i++) {
            // Process results.results[i]
        }
        streamdb_free_search_results(&results);
    }
    
    streamdb_close(db);
    return 0;
}
```

Compile with:

```bash
gcc -o example example.c -L target/release -lstreamdb -lpthread -ldl -lm
```

## Benchmarks

Run benchmarks:

```bash
cargo bench
```

### Results (Indicative)

*AMD Ryzen 7 5800X, 32GB RAM, release build*

| Operation | Throughput |
|-----------|------------|
| Insert (100B value) | 1.2M ops/sec |
| Get (cached) | 3.5M ops/sec |
| Get (cold) | 850K ops/sec |
| Suffix search (200 matches) | 120K ops/sec |

## API Reference

### Core Types

```rust
/// Main database handle
pub struct StreamDb { ... }

/// Configuration options
pub struct Config {
    pub page_size: usize,        // File page size (default: 4096)
    pub cache_size: usize,       // LRU cache entries (default: 10000)
    pub flush_interval_ms: u64,  // Auto-flush interval (default: 5000)
    pub use_mmap: bool,          // Memory-mapped I/O (default: true)
}

/// Search result
pub struct SearchResult {
    pub key: Vec<u8>,
    pub id: Uuid,
}
```

### Methods

| Method | Description |
|--------|-------------|
| `open(path, config)` | Open file-backed database |
| `open_memory()` | Open in-memory database |
| `insert(key, value)` | Insert or update |
| `get(key)` | Retrieve value |
| `exists(key)` | Check existence |
| `delete(key)` | Remove key-value |
| `suffix_search(suffix)` | Find keys by suffix |
| `for_each(callback)` | Iterate all entries |
| `flush()` | Persist to disk |
| `stats()` | Get statistics |
| `len()` / `is_empty()` | Key count |

## Error Handling

```rust
use streamdb::{Error, Result};

fn example() -> Result<()> {
    let db = StreamDb::open_memory()?;
    
    match db.get(b"missing") {
        Ok(Some(value)) => println!("Found: {:?}", value),
        Ok(None) => println!("Not found"),
        Err(Error::Corrupted(msg)) => eprintln!("Data corruption: {}", msg),
        Err(e) => eprintln!("Error: {}", e),
    }
    
    Ok(())
}
```

## Testing

```bash
# Run all tests
cargo test

# Run with all features
cargo test --all-features

# Run specific test
cargo test test_suffix_search
```

## License

GNU Lesser General Public License v2.1 (LGPL-2.1-or-later)

Copyright (C) 2025 DeMoD LLC

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run `cargo fmt` and `cargo clippy`
4. Ensure all tests pass
5. Submit a pull request
