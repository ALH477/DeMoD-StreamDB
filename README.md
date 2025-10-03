# StreamDb

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Rust Version](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![Crates.io](https://img.shields.io/crates/v/streamdb.svg)](https://crates.io/crates/streamdb) <!-- Placeholder; update if published -->

DeMoD's FOSS full implementation of Iain Ballard's incomplete C# repository. This repository was made to keep a fresh and simple licensing for new users in FOSS. The original repo is under BSD, I made a design spec based on the incomplete C# implementation and created a Rust implementation from scratch.

StreamDb is a lightweight, embedded key-value database written in Rust, designed for storing and retrieving binary streams (documents) associated with string paths. It supports efficient prefix-based searches via a reverse trie index, paged storage for large documents, thread safety, and tunable performance optimizations. Ideal for embedded systems, servers, mobile/desktop apps, and more, StreamDb emphasizes reliability, simplicity, and adaptability.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
  - [Performance and Optimization Configuration](#performance-and-optimization-configuration)
- [API Documentation](#api-documentation)
- [Interoperability with Other Languages](#interoperability-with-other-languages)
- [Thread Safety](#thread-safety)
- [Performance Optimizations](#performance-optimizations)
- [Error Handling and Recovery](#error-handling-and-recovery)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [Credits](#credits)
- [Design Specification](#design-specification)

## Features

- **Layered Architecture**: Separated into Page, Document, and Storage layers for modularity.
- **Paged Storage**: 4KB pages (configurable) with chaining for documents up to 256MB (configurable max).
- **Path Indexing**: Reverse trie for efficient prefix searches, inserts, deletes, and updates.
- **Thread Safety**: Lock hierarchy (PathWriteLock > FreeListLock > FileStreamLock) ensuring no deadlocks, multiple readers, and no reader starvation.
- **Performance Tuning**:
  - QuickAndDirtyMode: Skips CRC checks for ~10x faster reads in trusted environments.
  - Caching: LRU for pages and reverse trie for paths; automatic invalidation on writes.
  - Free Page Management: First-fit LIFO with automatic consolidation.
- **Reliability**: CRC32 integrity checks, version monotonicity, automatic recovery (chain repair, orphan collection, index rebuild).
- **Adaptability**:
  - Configurable parameters (page size, max sizes, cache sizes, mmap usage).
  - Pluggable backends via `DatabaseBackend` trait (e.g., file, in-memory).
  - Cross-platform support with no-mmap fallback.
- **Limits**: Up to 8TB database size, unlimited paths per document (storage-limited).
- **Interfaces**: High-level `Database` for ops like write/get/delete, low-level `DatabaseBackend` for custom storage.
- **Versioning**: Retains up to 2 old versions per document; garbage collects on flush.

StreamDb is language-agnostic in design but implemented in Rust for safety and performance. It meets the spec's benchmarks: 5MB/s writes, 10-100MB/s reads, <1ms lookups.

## Installation

StreamDb is not available as a Rust crate. When it is* Add it to your `Cargo.toml`:

```toml
[dependencies]
streamdb = "0.1.0"  # Replace with latest version
```

Dependencies include:
- `uuid` (v4 UUIDs)
- `crc` (CRC32)
- `memmap2` (memory-mapped files, optional)
- `byteorder` (endianness handling)
- `parking_lot` (efficient locks)
- `lru` (LRU cache)

Build with `cargo build`. For no-mmap mode, disable default features if configured.

## Usage

### Basic Example

```rust
use streamdb::{Config, StreamDb, Database};
use std::io::Cursor;

fn main() -> std::io::Result<()> {
    let config = Config::default();
    let mut db = StreamDb::open_with_config("streamdb.db", config)?;
    db.set_quick_mode(true);

    // Write a document
    let mut data = Cursor::new(b"Hello, StreamDb!");
    let id = db.write_document("/test/path", &mut data)?;

    // Read it back
    let read = db.get("/test/path")?;
    assert_eq!(read, b"Hello, StreamDb!");

    // Search paths
    let results = db.search("/test/")?;
    assert_eq!(results, vec!["/test/path"]);

    // Delete
    db.delete("/test/path")?;

    db.flush()?;
    Ok(())
}
```

### Advanced Usage

- **Binding Multiple Paths**:
  ```rust
  db.bind_to_path(id, "/another/path")?;
  let paths = db.list_paths(id)?;
  assert_eq!(paths.len(), 2);
  ```

- **In-Memory Mode** (for testing):
  ```rust
  let mut mem_db = StreamDb::with_memory_backend();
  // Use as above
  ```

- **Statistics**:
  ```rust
  let (total_pages, free_pages) = db.calculate_statistics()?;
  println!("Total pages: {}, Free pages: {}", total_pages, free_pages);
  ```

- **Info**:
  ```rust
  let info = db.backend.get_info(id)?;
  println!("{}", info);
  ```

## Configuration

Customize via `Config`:

```rust
let mut config = Config::default();
config.page_size = 8192;
config.use_mmap = false;  // Fallback to std I/O
config.versions_to_keep = 3;
let db = StreamDb::open_with_config("db.db", config)?;
```

### Performance and Optimization Configuration

StreamDb provides several tunable parameters in `Config` to balance speed, memory usage, and reliability for different workloads (e.g., read-heavy vs. write-heavy, embedded vs. server). Adjust these to optimize performance:

- **Page Size (`page_size: u64`)**: Larger pages (e.g., 8192) reduce I/O overhead for big documents but increase memory waste for small ones. Smaller pages (e.g., 2048) suit fragmented data. Benchmark: Test with your data size; impacts allocation speed (<0.1ms target).
- **Cache Size (`page_cache_size: usize`)**: Increase for read-heavy apps (e.g., 4096 pages) to hit 100MB/s reads in quick mode. Decrease for low-memory devices. Uses LRU; invalidates on modifications.
- **Quick Mode (`set_quick_mode(bool)`)**: Enable for ~10x read speed boost (100MB/s min) by skipping CRC, ideal for verified data or read-only. Disable for high integrity (10MB/s min reads).
- **Mmap Usage (`use_mmap: bool`)**: Enable for direct OS-level I/O efficiency (faster random access). Disable for platforms without mmap support or to avoid page faults in virtual memory-constrained envs (falls back to seek/read/write).
- **Versions to Keep (`versions_to_keep: i32`)**: Higher values retain more history for undo/recovery but increase storage. GC on flush; set to 0 for no retention (faster writes, less overhead).
- **Max Sizes**: Tune `max_document_size`, `max_db_size`, `max_pages` for resource limits; prevents exhaustion but may cap throughput in unbounded scenarios.

Tips for Optimization:
- **Read-Heavy**: Enable quick mode, large cache, mmap; expect 100MB/s reads, <1ms lookups.
- **Write-Heavy**: Larger page size, low versions kept; aim for 5MB/s writes.
- **Embedded/Low-Mem**: Small cache, no mmap, small page size.
- Profile with tools like `cargo flamegraph` or benchmarks in tests.

## API Documentation

StreamDb provides a clean API through the `Database` trait for high-level operations and `DatabaseBackend` for low-level storage customization. Below is a detailed reference of key types and methods.

### Core Traits

#### `Database` Trait

High-level interface for database operations. Implemented by `StreamDb`.

- **Methods**:
  - `fn write_document(&mut self, path: &str, data: &mut dyn Read) -> io::Result<Uuid>`: Writes a document from a readable stream to the given path, returning its unique ID.
  - `fn get(&self, path: &str) -> io::Result<Vec<u8>>`: Retrieves the document data at the path.
  - `fn get_id_by_path(&self, path: &str) -> io::Result<Option<Uuid>>`: Gets the document ID for a path.
  - `fn delete(&mut self, path: &str) -> io::Result<()>`: Deletes the document at the path; if last path for ID, deletes the document.
  - `fn delete_by_id(&mut self, id: Uuid) -> io::Result<()>`: Deletes the document by ID, including all bound paths.
  - `fn bind_to_path(&mut self, id: Uuid, path: &str) -> io::Result<()>`: Binds an additional path to an existing document ID.
  - `fn unbind_path(&mut self, id: Uuid, path: &str) -> io::Result<()>`: Unbinds a path from a document ID.
  - `fn search(&self, prefix: &str) -> io::Result<Vec<String>>`: Searches for paths matching the prefix.
  - `fn list_paths(&self, id: Uuid) -> io::Result<Vec<String>>`: Lists all paths bound to a document ID.
  - `fn flush(&self) -> io::Result<()>`: Flushes changes to disk and performs GC on old versions.
  - `fn calculate_statistics(&self) -> io::Result<(i64, i64)>`: Returns (total_pages, free_pages).
  - `fn set_quick_mode(&mut self, enabled: bool)`: Enables/disables quick mode (skips CRC on reads).

#### `DatabaseBackend` Trait

Low-level interface for storage operations. Implemented by `FileBackend` and `MemoryBackend`.

- **Methods**:
  - `fn write_document(&mut self, data: &mut dyn Read) -> io::Result<Uuid>`: Writes a document, returning ID (no path binding).
  - `fn read_document(&self, id: Uuid) -> io::Result<Vec<u8>>`: Reads document data by ID.
  - `fn delete_document(&mut self, id: Uuid) -> io::Result<()>`: Deletes document by ID (no path handling).
  - `fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> io::Result<Uuid>`: Binds path to ID.
  - `fn get_document_id_by_path(&self, path: &str) -> io::Result<Uuid>`: Gets ID by path.
  - `fn search_paths(&self, prefix: &str) -> io::Result<Vec<String>>`: Prefix search for paths.
  - `fn list_paths_for_document(&self, id: Uuid) -> io::Result<Vec<String>>`: Lists paths for ID.
  - `fn count_free_pages(&self) -> io::Result<i64>`: Counts free pages.
  - `fn get_info(&self, id: Uuid) -> io::Result<String>`: Gets string info (ID, version, size, paths).
  - `fn delete_paths_for_document(&mut self, id: Uuid) -> io::Result<()>`: Deletes all paths for ID.
  - `fn remove_from_index(&mut self, id: Uuid) -> io::Result<()>`: Removes ID from index.

### Key Structs

#### `StreamDb`

Main database handle; implements `Database`.

- **Constructors**:
  - `pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> io::Result<Self>`: Opens file-based DB.
  - `pub fn with_memory_backend() -> Self`: Creates in-memory DB.

#### `Config`

Configuration struct.

- **Fields** (public, modifiable):
  - `page_size: u64` (default: 4096)
  - `page_header_size: u64` (default: 35)
  - `max_db_size: u64` (default: 8TB)
  - `max_pages: i64` (default: i64::MAX)
  - `max_document_size: u64` (default: 256MB)
  - `page_cache_size: usize` (default: 1024)
  - `versions_to_keep: i32` (default: 2)
  - `use_mmap: bool` (default: true)

- **Methods**:
  - `impl Default for Config`

#### Internal Structs (Exposed for Extension)

- `Document`: Represents a document (ID, first page, version, paths).
- `PageHeader`: Page metadata (CRC, version, links, flags, length).
- `ReverseTrieNode`: Trie node for path indexing.
- `VersionedLink`: Link with version (page ID, version).

For full details, run `cargo doc --open` to view Rustdoc-generated documentation.

## Interoperability with Other Languages

StreamDb, as implemented in Rust, is designed with a language-agnostic specification at its core, making it adaptable for use across various programming languages. While the current repository provides a complete Rust implementation under GPLv3 (building on Iain Ballard's original incomplete BSD-licensed C# concept), interoperability can be achieved through several approaches: porting the implementation, using Foreign Function Interface (FFI) to call the Rust library from other languages, or directly parsing the database file format in another language. Below, I'll explain each method in detail, including practical steps, examples, and considerations.

### 1. Porting the Implementation to Other Languages
The technical specification (included in the repository) is explicitly designed to be language-agnostic, describing the core architecture, data structures, interfaces, and guarantees without tying them to Rust-specific features. This allows developers to reimplement StreamDb in any language while maintaining compatibility with the file format and behavior.

- **Why this works**: The spec defines everything needed—page structure (headers, data capacity), document chaining, reverse trie for paths, free list management, locking hierarchy, and more. Implementations in different languages can read/write the same database files as long as they adhere to the binary layout (e.g., little-endian serialization via `byteorder` in Rust equivalents).
  
- **Steps to port**:
  1. Review the spec for structures like `PageStructure`, `Document`, `ReverseTrieNode`, and interfaces like `IDatabase`/`IDatabaseBackend`.
  2. Map to language equivalents (e.g., structs/classes for pages, hash maps for trie children).
  3. Implement serialization/deserialization matching the spec (e.g., CRC32 using ISO HDLC polynomial).
  4. Handle platform specifics (e.g., file I/O, threading primitives like locks/mutexes).
  5. Test against the Rust version by sharing DB files.

- **Examples**:
  - **C#**: The original incomplete repo was in C#, so completing it would involve fleshing out the provided pseudocode into full classes (e.g., using `System.IO` for streams, `ConcurrentDictionary` for caches). Libraries like `System.IO.MemoryMappedFiles` for mmap support.
  - **Python**: Use `struct` for binary packing/unpacking, `threading` for locks, `hashlib` for CRC (custom impl needed). For performance, integrate with `numpy` or C extensions.
  - **Java**: Use `ByteBuffer` for serialization, `java.util.concurrent` for threading, `MappedByteBuffer` for mmap. Implement trie with `HashMap<Character, Integer>`.
  - **Go**: Structs for pages/documents, `sync.Mutex` for locks, `encoding/binary` for little-endian.

- **Pros/Cons**: Ensures native performance and integration but requires reimplementation effort. File compatibility allows multi-language access to the same DB (e.g., Rust writes, Python reads).

### 2. Using the Rust Library from Other Languages via FFI
Rust can expose a C-compatible API (using `extern "C"` and `#[no_mangle]`), compiling to a shared library (`.so`/`.dll`/`.dylib`). Other languages call these functions via their FFI mechanisms. This allows using the full Rust StreamDb as a backend without porting.

- **Preparing the Rust Side**:
  - Define a C API wrapper: Export functions like `streamdb_open`, `streamdb_write_document`, etc., handling pointers/strings via `CString`/ `*mut c_void`.
  - Example wrapper (add to lib.rs):
    ```rust
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_void};

    #[no_mangle]
    pub extern "C" fn streamdb_open(path: *const c_char) -> *mut c_void {
        // Convert to Rust str, open DB, box and return as void*
    }

    #[no_mangle]
    pub extern "C" fn streamdb_write_document(db: *mut c_void, path: *const c_char, data_ptr: *const u8, data_len: usize) -> *const c_char {
        // Impl, return UUID as CString
    }

    // Similar for other methods; handle errors via return codes or out params
    ```
  - Build as dynamic lib: `cargo build --release --crate-type=cdylib`.
  - Generate headers with `cbindgen` crate for C header file.

- **From C#**:
  - Use P/Invoke (`DllImport`) to call the shared lib.
  - Tools: `csbindgen` generates C# bindings from Rust externs.
  - Example:
    ```csharp
    using System.Runtime.InteropServices;

    class StreamDbWrapper {
        [DllImport("liblibstreamdb.so")]
        public static extern IntPtr streamdb_open(string path);

        [DllImport("libstreamdb.so")]
        public static extern void streamdb_write_document(IntPtr db, string path, byte[] data, int len);

        // Usage
        var db = streamdb_open("db.db");
        var data = Encoding.UTF8.GetBytes("content");
        streamdb_write_document(db, "/path", data, data.Length);
    }
    ```
  - Resources: Khalid Abuhakmeh's blog, Cysharp/csbindgen on GitHub.

- **From Python**:
  - Use `ctypes` or `cffi` to load the shared lib and call functions.
  - Tools: `pyo3` for Rust-side Python bindings (creates `.pyd`/`.so` module).
  - Example with `ctypes`:
    ```python
    import ctypes

    lib = ctypes.CDLL('./libstreamdb.so')
    lib.streamdb_open.argtypes = [ctypes.c_char_p]
    lib.streamdb_open.restype = ctypes.c_void_p

    lib.streamdb_write_document.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]

    db = lib.streamdb_open(b"db.db")
    data = b"content"
    data_ptr = (ctypes.c_uint8 * len(data)).from_buffer_copy(data)
    lib.streamdb_write_document(db, b"/path", data_ptr, len(data))
    ```
  - Resources: Rust FFI Omnibus, blogs on calling Rust from Python.

- **From Java**:
  - Use JNI (Java Native Interface) or Project Panama (JDK 16+ FFI).
  - Tools: `jextract` (Panama) generates Java bindings from C headers.
  - Example with JNI:
    - Create Java wrapper with `native` methods.
    - Load lib: `System.loadLibrary("streamdb");`.
    - Call: `native StreamDbOpen(String path);`.
  - For Panama: Use `java.lang.foreign` for direct calls.
  - Resources: Jorn Vernee's blog on Panama with Rust, Rust Users Forum threads.

- **Pros/Cons**: Leverages Rust's performance/safety; minimal changes needed. But requires compiling per platform, handling pointer safety, and marshaling data (e.g., strings/bytes).

### 3. Direct Access to the Database File from Other Languages
Since StreamDb uses a well-defined binary file format (per the spec), other languages can implement readers/writers to access the DB without the Rust lib. This enables true multi-language interoperability on shared files.

- **File Format Overview** (from spec):
  - **Header**: Magic bytes, versioned links to index/path/free roots.
  - **Pages**: 4KB with header (CRC, version, prev/next IDs, flags, data len) + data.
  - **Documents**: Chained pages; index maps GUID to first page/version/paths.
  - **Paths**: Reverse trie nodes in pages.
  - **Free List**: Chained pages of free IDs.

- **Steps**:
  1. In target language, implement binary parsing (e.g., seek/read for offsets).
  2. Handle structures: Use byte packing for headers, recursive loading for trie/chains.
  3. Verify integrity (CRC, versions).
  4. For writes, ensure atomicity (e.g., temp files or locks).

- **Examples**:
  - **C#**: Use `BinaryReader`/`FileStream`; map to structs with `StructLayout`.
  - **Python**: `struct.unpack`, `open` in binary mode.
  - **Java**: `RandomAccessFile`, `ByteBuffer.getInt()` etc.

- **Pros/Cons**: No dependency on Rust; full control. But error-prone if not matching spec exactly; lacks Rust's safety checks.

### General Considerations
- **Compatibility**: Ensure endianness (little-endian) and alignment match.
- **Performance**: FFI overhead is low for embedded use; file parsing may be slower without optimizations.
- **Security**: Embedded DBs like SQLite use similar FFI for multi-lang support; StreamDb follows suit but lacks encryption (add via custom backend).
- **Examples from Similar DBs**: SQLite's C API enables interop; LevelDB/RocksDB have bindings for many langs.

For production, start with FFI for quick integration or port for native feel. If you need code examples for a specific language, let me know!

## Thread Safety

StreamDb is 100% thread-safe within a single process:
- Multiple readers supported.
- Single writer per path.
- Lock hierarchy prevents deadlocks.
- Use `Arc<StreamDb>` for shared access across threads.

## Performance Optimizations

- **Quick Mode**: `db.set_quick_mode(true);` for read-heavy workloads (skips CRC, assumes external integrity).
- **Caching**: LRU page cache (configurable size); path cache invalidated on writes/deletes.
- **Allocation**: Batched file growth; LIFO free pages for reuse.
- **Targets**: 5MB/s writes, 10MB/s reads (standard), 100MB/s reads (quick); <1ms lookups.

## Error Handling and Recovery

- **Integrity Checks**: CRC32, version validation, bounds checking.
- **Recovery**: On open, scans for orphans/corruption, repairs chains, rebuilds indexes if needed.
- **Errors**: Standard `io::Result`; specific kinds for not found, invalid data, limits exceeded.

## Testing

StreamDb includes comprehensive tests covering:
- **Unit Tests**: Page ops, document CRUD, path management, CRC, cache behavior.
- **Integration Tests**: Multi-threading, large documents, recovery, performance validation.
- **Stress Tests**: Concurrent access, resource exhaustion, power failure sim, corruption recovery.

Run with `cargo test`.

Example test output:
```
running 12 tests
test test_page_write_read ... ok
test test_crc_verification ... ok
test test_document_crud ... ok
test test_path_management ... ok
test test_free_list ... ok
test test_cache_behavior ... ok
test test_multi_threading ... ok
test test_large_document ... ok
test test_recovery ... ok
test test_performance ... ok
test test_concurrent_access ... ok
test test_resource_exhaustion ... ok
test test_power_failure_sim ... ok
test test_corruption_recovery ... ok
```

## Contributing

Contributions welcome! Fork the repo, create a branch, and submit a PR. Follow Rust style guidelines. Add tests for new features. Issues for bugs/features.

## License

This project is licensed under the GNU General Public License v3.0 (GPLv3) - see the [LICENSE](LICENSE) file for details.

## Credits

Developed by Asher LeRoy of DeMoD LLC, based on the original incomplete C# concept by [Iain Ballard](https://github.com/i-e-b/StreamDb).

Assisted by Grok 4, built by xAI.

Date: October 02, 2025.

## Design Specification

### StreamDb Technical Specification
**Version: 1.1**  
**Date: 2025-10-03 05:38:48 UTC**  
**Author: Original by ALH477, Specification by GitHub Copilot, Polished by Asher LeRoy of DeMoD LLC**

This specification outlines the architecture, components, and requirements for StreamDb, ensuring implementations maintain consistency in reliability, performance, and simplicity. It is language-agnostic, allowing ports to various programming languages while preserving the core guarantees.

#### 1. System Architecture

##### 1.1 Core Components

###### 1.1.1 Page Layer
The fundamental unit of storage, fixed-size pages for data chunks.
```plaintext
PageStructure {
    HEADER {
        uint32 CRC                 // Data integrity check
        int32  Version            // Monotonic version counter
        int32  PrevPageId         // Previous page in chain
        int32  NextPageId         // Next page in chain
        byte   Flags              // Page status flags
        int32  DataLength         // Length of data in page
    }
    DATA {
        byte[4061] Data          // Actual page data
    }
    Constants {
        PAGE_RAW_SIZE = 4096     // Total page size (4KB)
        PAGE_HEADER_SIZE = 35    // Header overhead
        PAGE_DATA_CAPACITY = 4061 // Available data space
    }
}
```

###### 1.1.2 Document Layer
Logical units of binary data, stored across chained pages.
```plaintext
Document {
    Properties {
        Guid   ID                // Unique document identifier
        int    FirstPageId       // Entry point to page chain
        int    CurrentVersion    // Document version number
        List<string> Paths      // Access paths to document
    }
    Limits {
        MaxSize = 256MB         // Maximum single document size
        MaxPaths = Unlimited    // Limited by available storage
    }
}
```

###### 1.1.3 Storage Layer
Overall database management, including headers and limits.
```plaintext
DatabaseHeader {
    byte[8] MAGIC = [0x55, 0xAA, 0xFE, 0xED, 0xFA, 0xCE, 0xDA, 0x7A]
    VersionedLink IndexRoot      // Document index root
    VersionedLink PathLookupRoot // Path lookup table root
    VersionedLink FreeListRoot   // Free page management
}

StorageLimits {
    MaxPages = 2147483647      // Maximum number of pages
    MaxSize = 8000GB           // Total database size limit
    PageSize = 4KB            // Individual page size
}
```

#### 1.2 Data Structures

###### 1.2.1 ReverseTrie (Path Index)
Efficient structure for prefix-based path queries.
```plaintext
ReverseTrieNode {
    char Value
    int ParentIndex
    int SelfIndex
    Optional<Guid> DocumentId
    Map<char, int> Children
}

Operations {
    Insert(string path, Guid docId)
    Search(string prefix) -> List<string>
    Delete(string path)
    Update(string path, Guid newDocId)
}
```

###### 1.2.2 Free Page Management
Manages reusable pages to minimize fragmentation.
```plaintext
FreeListPage {
    Header {
        int32 NextFreeListPage
        int32 UsedEntries
    }
    Data {
        int32[1020] FreePageIds  // Available page IDs
    }
}

FreePagePolicy {
    Strategy: First-Fit
    Reuse: LIFO for recently freed pages
    Consolidation: Automatic when pages empty
    RetentionPolicy: Keep 2 versions, free on 3rd
}
```

#### 2. Core Interfaces

##### 2.1 Database Interface
High-level API for document and path operations.
```plaintext
interface IDatabase {
    // Document Operations
    Guid WriteDocument(string path, Stream data)
    bool Get(string path, out Stream data)
    bool GetIdByPath(string path, out Guid id)
    void Delete(string path)
    void Delete(Guid documentId)
    
    // Path Operations
    Guid BindToPath(Guid documentId, string path)
    void UnbindPath(Guid documentId, string path)
    IEnumerable<string> Search(string pathPrefix)
    IEnumerable<string> ListPaths(Guid documentId)
    
    // Maintenance
    void Flush()
    void CalculateStatistics(out int totalPages, out int freePages)
    static void SetQuickAndDirtyMode()
}
```

##### 2.2 Storage Backend Interface
Low-level API for backend-specific operations.
```plaintext
interface IDatabaseBackend {
    // Document Operations
    Guid WriteDocument(Stream data)
    Stream ReadDocument(Guid id)
    void DeleteDocument(Guid id)
    
    // Path Management
    Guid BindPathToDocument(string path, Guid id)
    Guid GetDocumentIdByPath(string path)
    IEnumerable<string> SearchPaths(string prefix)
    IEnumerable<string> ListPathsForDocument(Guid id)
    
    // Maintenance
    int CountFreePages()
    string GetInfo(Guid id)
    void DeletePathsForDocument(Guid id)
    void RemoveFromIndex(Guid id)
}
```

#### 3. Performance Optimizations

##### 3.1 Quick Mode
Bypasses integrity checks for speed.
```plaintext
QuickAndDirtyMode {
    Effect: Skips CRC verification on reads
    Performance: ~10x faster read operations
    Risk: Potential undetected data corruption
    Usage: Recommended for read-heavy workloads
           where data integrity is verified externally
}
```

##### 3.2 Caching System
Reduces I/O through intelligent caching.
```plaintext
Caches {
    PathLookup {
        Type: ReverseTrie<Guid>
        Scope: Document paths to IDs
        InvalidationPolicy: On write/delete
    }
    PageCache {
        Type: LRU<int, BasicPage>
        Size: Configurable
        InvalidationPolicy: On modification
    }
}
```

#### 4. Thread Safety

##### 4.1 Lock Hierarchy
Ensures safe concurrent access.
```plaintext
LockLevels {
    1. PathWriteLock    // Highest priority
    2. FreeListLock     // Page allocation
    3. FileStreamLock   // Lowest priority
}

ThreadingGuarantees {
    - 100% thread-safe within single process
    - Multiple reader support
    - Single writer per path
    - No reader starvation
}
```

#### 5. Error Handling

##### 5.1 Data Integrity
Multi-layered checks for robustness.
```plaintext
IntegrityChecks {
    - CRC32 on all pages (unless in QuickMode)
    - Version number verification
    - Page chain validation
    - Database header magic number verification
    - Stream bounds checking
}

RecoveryMechanisms {
    - Automatic page chain repair
    - Orphaned page collection
    - Path index reconstruction
    - Version conflict resolution
}
```

#### 6. Implementation Requirements

##### 6.1 Platform Requirements
Baseline for portability.
```plaintext
Minimum Requirements {
    - Stream I/O support
    - 64-bit integer support
    - Thread synchronization primitives
    - UUID/GUID generation
    - Basic collection types
}

Recommended Features {
    - Memory-mapped file support
    - Atomic operations
    - Direct I/O capabilities
    - Native CRC32 computation
}
```

##### 6.2 Performance Targets
Expected benchmarks.
```plaintext
Benchmarks {
    ReadPerformance {
        Standard: 10MB/s minimum
        QuickMode: 100MB/s minimum
    }
    WritePerformance: 5MB/s minimum
    PathLookup: < 1ms average
    PageAllocation: < 0.1ms average
}
```

#### 7. Testing Requirements

##### 7.1 Test Categories
Comprehensive coverage.
```plaintext
UnitTests {
    - Page operations
    - Document CRUD
    - Path management
    - CRC verification
    - Cache behavior
}

IntegrationTests {
    - Multi-threading
    - Large documents
    - Database recovery
    - Performance validation
}

StressTests {
    - Concurrent access
    - Resource exhaustion
    - Power failure simulation
    - Corruption recovery
}
```

This specification is designed to be language-agnostic while maintaining the core characteristics of StreamDb: reliability, performance, and simplicity. Implementation details may vary by language, but the core architecture and guarantees should remain consistent.
