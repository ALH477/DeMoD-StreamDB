# StreamDB

A lightweight, thread-safe embedded key-value database implemented in C, using a reverse trie data structure for efficient suffix-based searches.

[![License: LGPL v2.1](https://img.shields.io/badge/License-LGPL_v2.1-blue.svg)](https://www.gnu.org/licenses/lgpl-2.1)
[![C Standard](https://img.shields.io/badge/C-C11-blue.svg)](https://en.wikipedia.org/wiki/C11_(C_standard_revision))
[![Version](https://img.shields.io/badge/version-2.0.0-green.svg)](CHANGELOG.md)

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Building](#building)
- [Design Specification](#design-specification)
  - [Architecture Overview](#architecture-overview)
  - [Data Structure: Reverse Trie](#data-structure-reverse-trie)
  - [Memory Layout](#memory-layout)
  - [Threading Model](#threading-model)
  - [Persistence Format](#persistence-format)
  - [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Performance Characteristics](#performance-characteristics)
- [Limitations](#limitations)
- [License](#license)

---

## Features

| Feature | Description |
|---------|-------------|
| **Thread-safe** | All operations protected by recursive mutex |
| **Suffix search** | O(m + k) lookup for keys ending with a given suffix |
| **Binary keys** | Supports arbitrary byte sequences, not just strings |
| **Auto-persistence** | Background thread periodically flushes to disk |
| **Cross-platform** | Windows, Linux, macOS, BSD |
| **Zero dependencies** | Pure C11 with standard library only |
| **Memory-only mode** | Optional in-memory operation without disk I/O |
| **Atomic writes** | Crash-safe persistence via temp file + rename |

---

## Quick Start

```c
#include <streamdb.h>
#include <stdio.h>
#include <stdlib.h>

int main(void) {
    // Initialize with file persistence, 5-second auto-flush
    StreamDB* db = streamdb_init("mydb.dat", 5000);
    
    // Insert key-value pairs
    streamdb_insert(db, (unsigned char*)"user:alice", 10, "Alice Smith", 12);
    streamdb_insert(db, (unsigned char*)"user:bob", 8, "Bob Jones", 10);
    
    // Retrieve (returns a copy - must free)
    size_t size;
    char* value = streamdb_get(db, (unsigned char*)"user:alice", 10, &size);
    if (value) {
        printf("Found: %s\n", value);
        free(value);
    }
    
    // Suffix search - find all keys ending with "alice"
    StreamDBResult* results = streamdb_suffix_search(db, 
        (unsigned char*)"alice", 5);
    for (StreamDBResult* r = results; r; r = r->next) {
        printf("Key: %.*s\n", (int)r->key_len, (char*)r->key);
    }
    streamdb_free_results(results);
    
    // Cleanup (auto-flushes if dirty)
    streamdb_free(db);
    return 0;
}
```

Compile and run:
```bash
gcc -o example example.c -lstreamdb -lpthread
./example
```

---

## Building

### Prerequisites

- C11-compatible compiler (GCC 4.9+, Clang 3.1+, MSVC 2015+)
- POSIX threads (pthreads) on Unix-like systems
- CMake 3.10+ (optional) or GNU Make

### Using Make

```bash
make              # Build static and shared libraries
make test         # Build and run test suite
make debug        # Build with AddressSanitizer + run tests
make install      # Install to /usr/local (or PREFIX=/path)
```

### Using CMake

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
ctest             # Run tests
sudo cmake --install .
```

#### CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `STREAMDB_BUILD_SHARED` | ON | Build shared library (.so/.dylib/.dll) |
| `STREAMDB_BUILD_STATIC` | ON | Build static library (.a/.lib) |
| `STREAMDB_BUILD_TESTS` | ON | Build test suite |
| `STREAMDB_INSTALL` | ON | Generate install targets |

### Using Nix

```bash
nix build              # Build default package
nix develop            # Enter development shell
nix flake check        # Run tests
```

---

## Design Specification

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         StreamDB                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Public API │  │   Wrapper   │  │   Platform Abstraction  │  │
│  │             │  │    API      │  │   (mutex, threads, UUID)│  │
│  └──────┬──────┘  └──────┬──────┘  └────────────┬────────────┘  │
│         │                │                      │               │
│         └────────────────┼──────────────────────┘               │
│                          │                                      │
│  ┌───────────────────────▼───────────────────────────────────┐  │
│  │                    Core Engine                            │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐    │  │
│  │  │ Reverse Trie│  │  Serializer │  │  Auto-Flush     │    │  │
│  │  │   (Data)    │  │ (Persistence│  │   Thread        │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘    │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Components:**

1. **Public API** (`streamdb.h`) — Core database operations
2. **Wrapper API** (`streamdb_wrapper.h`) — Convenience functions (documents, batch ops)
3. **Platform Abstraction** — Portable mutex, threads, condition variables, UUID
4. **Reverse Trie** — Primary data structure for storage and suffix search
5. **Serializer** — Binary persistence with versioned format
6. **Auto-Flush Thread** — Background persistence with graceful shutdown

---

### Data Structure: Reverse Trie

StreamDB uses a **reverse trie** (suffix trie) as its primary data structure. Keys are inserted in reverse byte order, enabling efficient suffix-based lookups.

#### Why Reverse Trie?

| Operation | Hash Table | B-Tree | Trie | Reverse Trie |
|-----------|------------|--------|------|--------------|
| Exact lookup | O(1) avg | O(log n) | O(m) | O(m) |
| Prefix search | O(n) | O(log n + k) | O(m + k) | O(n) |
| **Suffix search** | O(n) | O(n) | O(n) | **O(m + k)** |
| Range scan | O(n) | O(log n + k) | O(n) | O(n) |
| Insert | O(1) avg | O(log n) | O(m) | O(m) |
| Delete | O(1) avg | O(log n) | O(m) | O(m) |

*Where: n = total keys, m = key length, k = matching results*

**Use cases optimized for suffix search:**
- File extension lookups (`.txt`, `.json`)
- Domain name resolution (`*.example.com`)
- Log analysis by timestamp suffix
- Reverse DNS lookups

#### Trie Node Structure

```c
typedef struct TrieNode {
    struct TrieNode* children[256];  // One slot per byte value
    void* value;                      // Stored value (if terminal)
    size_t value_size;                // Size of value in bytes
    int is_end;                       // Terminal node flag
} TrieNode;
```

#### Insertion Example

Inserting keys `"cat"`, `"car"`, `"card"`:

```
Original keys:     Reversed for insertion:
  cat                tac
  car                rac  
  card               drac

Trie structure (reversed):

         [root]
         /    \
       [t]    [r]
        |      |
       [a]    [a]
        |      |
       [c]*   [c]*
               |
              [d]*

(* = terminal node with value)
```

**Suffix search for `"ar"`** (reversed: `"ra"`):
1. Traverse: root → `[r]` → `[a]`
2. Collect all terminals in subtree: `"car"`, `"card"`

---

### Memory Layout

#### Database Structure

```c
struct StreamDB {
    // Data
    TrieNode* root;           // Root of reverse trie (never NULL)
    size_t total_size;        // Sum of all value sizes
    size_t key_count;         // Number of stored keys
    size_t node_count;        // Number of allocated nodes
    
    // Synchronization
    mutex_t mutex;            // Recursive mutex for all operations
    condvar_t shutdown_cv;    // Condition variable for shutdown
    
    // Persistence
    char* file_path;          // Path to data file (NULL = memory-only)
    int is_file_backend;      // Quick check for persistence mode
    int dirty;                // Unflushed modifications exist
    int running;              // Auto-flush thread active
    int shutdown_requested;   // Graceful shutdown flag
    thread_t auto_thread;     // Background flush thread handle
    int auto_flush_interval_ms;
    int thread_started;       // Thread creation succeeded
};
```

#### Memory Overhead

| Component | Size (64-bit) | Notes |
|-----------|---------------|-------|
| TrieNode | 2,064 bytes | 256 pointers + value ptr + metadata |
| StreamDB | ~128 bytes | Fixed overhead per database |
| Per key | ~(depth × 2KB) | Shared nodes amortize cost |

**Memory optimization strategies:**
- Nodes are shared across keys with common suffixes
- Values are stored by reference (single allocation)
- Node count tracked for monitoring

---

### Threading Model

#### Concurrency Guarantees

1. **All public functions are thread-safe**
2. **Recursive mutex** allows nested calls within callbacks
3. **Copy-on-read** semantics — `streamdb_get()` returns independent copy
4. **Atomic persistence** — temp file + rename pattern

#### Lock Hierarchy

```
┌─────────────────────────────────────────────┐
│              db->mutex                      │
│  ┌───────────────────────────────────────┐  │
│  │  All trie operations                  │  │
│  │  All metadata updates                 │  │
│  │  Serialization (during flush)         │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

Single mutex design chosen for:
- Simplicity and correctness
- Low contention expected (embedded use case)
- Avoiding deadlock complexity

#### Auto-Flush Thread Lifecycle

```
streamdb_init()
      │
      ▼
┌─────────────────┐
│ Thread Created  │◄─────────────────────────┐
└────────┬────────┘                          │
         │                                   │
         ▼                                   │
┌─────────────────┐    timeout    ┌──────────┴────────┐
│ Wait on condvar │──────────────►│ Check dirty flag  │
└────────┬────────┘               └──────────┬────────┘
         │                                   │
         │ signal                            │ if dirty
         ▼                                   ▼
┌─────────────────┐               ┌─────────────────────┐
│ shutdown_requested?             │ Flush to disk       │
└────────┬────────┘               └──────────┬──────────┘
         │                                   │
         │ yes                               │
         ▼                                   │
┌─────────────────┐                          │
│  Thread Exit    │◄─────────────────────────┘
└─────────────────┘

streamdb_shutdown() ──► Sets shutdown_requested + signals condvar
streamdb_free() ──────► Calls shutdown + joins thread + final flush
```

**Graceful shutdown benefits:**
- Immediate wakeup via condition variable (vs. sleeping full interval)
- Final flush ensures no data loss
- Clean thread termination

---

### Persistence Format

#### File Structure (v2)

```
┌──────────────────────────────────────────┐
│  Magic Number: "STDB" (4 bytes)          │
├──────────────────────────────────────────┤
│  Version: int32 = 2 (4 bytes)            │
├──────────────────────────────────────────┤
│  Serialized Trie (recursive structure)   │
│  ┌────────────────────────────────────┐  │
│  │  Node:                             │  │
│  │    is_end: int32                   │  │
│  │    value_size: size_t              │  │
│  │    value: [value_size bytes]       │  │
│  │    child_count: int32              │  │
│  │    children: [                     │  │
│  │      byte_index: uint8             │  │
│  │      child_node: Node (recursive)  │  │
│  │    ] × child_count                 │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

#### Serialization Algorithm

Iterative depth-first traversal using explicit stack (avoids stack overflow):

```c
// Pseudocode for serialization
stack = [(root, WRITE_HEADER)]
while stack not empty:
    (node, state) = stack.pop()
    
    if state == WRITE_HEADER:
        write(node.is_end, node.value_size)
        if node.is_end: write(node.value)
        write(child_count)
        stack.push((node, 0))  // Start iterating children
        
    else:  // state = next child index to process
        for i in range(state, 256):
            if node.children[i]:
                write(i)  // Child byte index
                stack.push((node, i+1))  // Resume here later
                stack.push((node.children[i], WRITE_HEADER))
                break
```

#### Atomic Write Strategy

```
1. Write to: {file_path}.{pid}.tmp
2. Sync to disk (implicit on fclose)
3. Delete existing: {file_path}
4. Rename: {file_path}.{pid}.tmp → {file_path}
```

**Properties:**
- Crash during write leaves original intact
- PID in temp name prevents multi-process collision
- Rename is atomic on POSIX systems

---

### Error Handling

#### Status Codes

```c
typedef enum {
    STREAMDB_OK = 0,            // Success
    STREAMDB_ERROR = -1,        // General/unknown error
    STREAMDB_NOT_FOUND = -2,    // Key does not exist
    STREAMDB_INVALID_ARG = -3,  // NULL pointer or invalid parameter
    STREAMDB_NO_MEMORY = -4,    // malloc() failed
    STREAMDB_IO_ERROR = -5,     // File operation failed
    STREAMDB_NOT_SUPPORTED = -6 // Operation not applicable
} StreamDBStatus;
```

#### Error Handling Philosophy

1. **Never crash** — All NULL inputs handled gracefully
2. **Explicit status codes** — No errno dependency
3. **Partial failure handling** — Batch operations report first error
4. **Human-readable messages** — `streamdb_strerror()` for logging

---

## API Reference

### Initialization

| Function | Description |
|----------|-------------|
| `streamdb_init(path, interval_ms)` | Create database with optional file persistence |
| `streamdb_init_with_config(path, config)` | Create with extended configuration |
| `streamdb_free(db)` | Close database and free resources |
| `streamdb_shutdown(db)` | Signal background thread to stop |

### Core Operations

| Function | Description |
|----------|-------------|
| `streamdb_insert(db, key, key_len, value, value_size)` | Insert or update |
| `streamdb_get(db, key, key_len, &size)` | Retrieve copy of value |
| `streamdb_exists(db, key, key_len)` | Check if key exists |
| `streamdb_delete(db, key, key_len)` | Remove key-value pair |

### Search & Iteration

| Function | Description |
|----------|-------------|
| `streamdb_suffix_search(db, suffix, len)` | Find keys ending with suffix |
| `streamdb_foreach(db, callback, user_data)` | Iterate all entries |
| `streamdb_free_results(results)` | Free search result list |

### Persistence

| Function | Description |
|----------|-------------|
| `streamdb_flush(db)` | Force write to disk |
| `streamdb_get_stats(db, &stats)` | Get database statistics |

### Utilities

| Function | Description |
|----------|-------------|
| `streamdb_strerror(status)` | Get error message |
| `streamdb_version()` | Get library version string |

---

## Performance Characteristics

### Time Complexity

| Operation | Average | Worst Case | Notes |
|-----------|---------|------------|-------|
| Insert | O(m) | O(m) | m = key length |
| Get | O(m) | O(m) | Plus O(v) copy, v = value size |
| Delete | O(m) | O(m) | |
| Exists | O(m) | O(m) | No value copy |
| Suffix search | O(m + k) | O(m + n) | k = matches, n = total keys |
| Flush | O(N) | O(N) | N = total nodes |

### Space Complexity

| Metric | Complexity | Notes |
|--------|------------|-------|
| Node overhead | O(256 × ptr_size) | ~2KB per node on 64-bit |
| Total nodes | O(Σ key_lengths) | Upper bound, sharing reduces this |
| Value storage | O(Σ value_sizes) | Exact, no overhead |

### Benchmarks (Indicative)

*Tested on: AMD Ryzen 7 5800X, 32GB RAM, NVMe SSD, GCC 11 -O2*

| Operation | Throughput | Latency (p99) |
|-----------|------------|---------------|
| Insert (100B value) | 850K ops/sec | 12 µs |
| Get (100B value) | 1.2M ops/sec | 8 µs |
| Suffix search (10 matches) | 450K ops/sec | 25 µs |
| Flush (10K keys) | 180 ops/sec | 18 ms |

---

## Limitations

### Current Limitations

| Limitation | Value | Rationale |
|------------|-------|-----------|
| Max key length | 1,024 bytes | Stack allocation in search |
| Max value size | ~2GB | `size_t` on 32-bit systems |
| Concurrent writers | Serialized | Single mutex design |
| Transactions | Not supported | Simplicity |
| Compression | Not supported | Future enhancement |

### Known Trade-offs

1. **Memory usage** — 2KB per node is high for sparse key distributions
2. **Suffix-only search** — Prefix search requires full scan
3. **Single-file persistence** — No WAL, potential data loss window
4. **No range queries** — Trie structure doesn't support ordered iteration

### Future Enhancements

- [ ] Adaptive radix trie (reduce node memory)
- [ ] Write-ahead logging (reduce data loss window)  
- [ ] Read-write lock (improve read concurrency)
- [ ] Value compression (LZ4/Zstd)
- [ ] Memory-mapped I/O option
- [ ] Prefix search via separate forward trie

---

## License

GNU Lesser General Public License v2.1 (LGPL-2.1)

Copyright (C) 2025 DeMoD LLC

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Ensure all tests pass: `make test`
4. Run with sanitizers: `make debug`
5. Submit a pull request

**Code style:**
- C11 standard
- 4-space indentation
- `snake_case` for functions and variables
- `UPPER_CASE` for constants and macros
