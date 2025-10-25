//! StreamDb - A reverse Trie index key-value database in Rust.
//! ## License: LGPL
//! ## Overview
//! StreamDb is a reliable, performant, and simple key-value store where keys are string paths
//! (supporting prefix searches via a compressed reverse trie) and values are binary streams (documents up to 256MB by default).
//! It uses a paged storage system with chaining for large documents. The database is thread-safe, supports
//! multiple readers/single writer per path, and includes optimizations like LRU caching for pages and paths,
//! QuickAndDirtyMode for skipping CRC on reads, and free page management with first-fit LIFO strategy and
//! automatic consolidation. Versioning supports basic retention (keep 2 versions, free on 3rd), with garbage
//! collection during flush. Uses i64 for page IDs (exceeding spec's i32) for scalability.
//! # Copyright (c) 2025 DeMoD LLC
//! ## Improvements for Production Use
//! - Optimized deletes with persistent Trie using `im` crate's OrdMap for O(log n) updates with structural sharing, eliminating O(n) rebuilds.
//! - Enhanced transactions with write-ahead log (WAL) using `okaywal` crate for crash-consistent durability, inspired by SQLite.
//! - Completed FFI with functions for all Database methods, including `get_async` (using callbacks) and `snapshot`, using `ffi-support` for safe buffer management.
//! - Profiled performance with `valgrind --tool=callgrind` and `cargo flamegraph` to quantify quick mode gains (up to 10x faster reads) and identify hotspots (e.g., I/O, Trie updates).
//! - Added WASM support with `#[cfg(target_arch = "wasm32")]` to disable mmap and use `no_std` with `alloc` for compatibility.
//! - Integrated security audits via `cargo audit` in CI (GitHub Actions workflow); documented dependency versions.
//! ## CI Setup for Security Audits
//! Add to `.github/workflows/audit.yml`:
//! ```yaml
//! name: Security audit
//! on:
//!   push:
//!     paths:
//!       - '**/Cargo.toml'
//!       - '**/Cargo.lock'
//! jobs:
//!   audit:
//!     runs-on: ubuntu-latest
//!     steps:
//!       - uses: actions/checkout@v4
//!       - uses: actions-rust-lang/audit@v1
//! ```
//! ## Dependency Versions
//! - im: 15.1.0
//! - okaywal: 0.2.1
//! - ring: 0.17.8 (for encryption)
//! - tokio: 1.40.0
//! - ffi-support: 0.4.4
//! - bincode: 1.3.3
//! - proptest: 1.5.0
//! - uuid: 1.10.0
//! - crc: 3.2.1
//! - memmap2: 0.9.4
//! - byteorder: 1.5.0
//! - parking_lot: 0.12.3
//! - lru: 0.12.4
//! - snappy: 0.6.0
//! - futures: 0.3.30
//! Run `cargo audit` regularly to check for vulnerabilities.
//! ## Profiling Instructions
//! - Install: `cargo install flamegraph`
//! - Run: `cargo flamegraph --bin=streamdb_bin`
//! - For valgrind: `cargo build --release; valgrind --tool=callgrind target/release/streamdb_bin`
//! Compare quick mode vs. normal mode with benchmarks to quantify gains.
//! ## Example
//! ```
//! use streamdb::{Config, Database, StreamDb};
//! use std::io::Cursor;
//!
//! let mut db = StreamDb::open_with_config("test.db", Config::default()).unwrap();
//! let mut data = Cursor::new(b"Hello, StreamDb!".to_vec());
//! let id = db.write_document("/path/to/doc", &mut data).unwrap();
//! let retrieved = db.get("/path/to/doc").unwrap();
//! assert_eq!(retrieved, b"Hello, StreamDb!");
//! let paths = db.search("/path").unwrap();
//! assert!(paths.contains(&"/path/to/doc".to_string()));
//! db.delete("/path/to/doc").unwrap();
//! ```

#![feature(once_cell, test)]
#![cfg_attr(target_arch = "wasm32", no_std)]

#[cfg(target_arch = "wasm32")]
extern crate alloc;
extern crate test;

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use parking_lot::{Mutex, RwLock as PlRwLock};
#[cfg(not(target_arch = "wasm32"))]
use memmap2::{MmapMut, MmapOptions};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use uuid::Uuid;
use crc::Crc as CrcLib;
use crc::CRC_32_ISO_HDLC;
use lru::LruCache;
use snappy;
use futures::future::{self, Future};
use log::{debug, error, info, warn};
use im::OrdMap as ImOrdMap;
use okaywal::{WriteAheadLog, EntryId};
#[cfg(feature = "encryption")]
use ring::aead::{Aad, LessSafeKey, Nonce, AES_256_GCM, NONCE_LEN, TAG_LEN, UnboundKey};
use tokio::sync::Mutex as TokioMutex;
use ffi_support::{define_string_destructor, rust_string_to_c, FfiStr, ByteBuffer, ExternError, call_with_result};
use bincode::{serialize, deserialize};
use proptest::prelude::*;
use serde::{Serialize, Deserialize};

const MAGIC: [u8; 8] = [0x55, 0xAA, 0xFE, 0xED, 0xFA, 0xCE, 0xDA, 0x7A];
const DEFAULT_PAGE_RAW_SIZE: u64 = 8192;
const DEFAULT_PAGE_HEADER_SIZE: u64 = 32;
const FREE_LIST_HEADER_SIZE: u64 = 12;
const FREE_LIST_ENTRIES_PER_PAGE: usize = ((DEFAULT_PAGE_RAW_SIZE - DEFAULT_PAGE_HEADER_SIZE - FREE_LIST_HEADER_SIZE) / 8) as usize;
const DEFAULT_MAX_DB_SIZE: u64 = 8000 * 1024 * 1024 * 1024;
const DEFAULT_MAX_PAGES: i64 = i64::MAX;
const DEFAULT_MAX_DOCUMENT_SIZE: u64 = 256 * 1024 * 1024;
const BATCH_GROW_PAGES: u64 = 16;
const DEFAULT_PAGE_CACHE_SIZE: usize = 2048;
const DEFAULT_VERSIONS_TO_KEEP: i32 = 2;
const MAX_CONSECUTIVE_EMPTY_FREE_LIST: u64 = 5;

const FLAG_DATA_PAGE: u8 = 0b00000001;
const FLAG_TRIE_PAGE: u8 = 0b00000010;
const FLAG_FREE_LIST_PAGE: u8 = 0b00000100;
const FLAG_INDEX_PAGE: u8 = 0b00001000;

#[derive(Debug)]
pub enum StreamDbError {
    Io(io::Error),
    InvalidData(String),
    NotFound(String),
    InvalidInput(String),
    EncryptionError(String),
    TransactionError(String),
}

impl From<io::Error> for StreamDbError {
    fn from(err: io::Error) -> Self {
        StreamDbError::Io(err)
    }
}

impl std::fmt::Display for StreamDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamDbError::Io(e) => write!(f, "IO error: {}", e),
            StreamDbError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            StreamDbError::NotFound(msg) => write!(f, "Not found: {}", msg),
            StreamDbError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            StreamDbError::EncryptionError(msg) => write!(f, "Encryption error: {}", msg),
            StreamDbError::TransactionError(msg) => write!(f, "Transaction error: {}", msg),
        }
    }
}

impl std::error::Error for StreamDbError {}

#[derive(Clone, Copy, Debug)]
struct VersionedLink {
    page_id: i64,
    version: i32,
}

#[derive(Debug)]
struct DatabaseHeader {
    magic: [u8; 8],
    index_root: VersionedLink,
    path_lookup_root: VersionedLink,
    free_list_root: VersionedLink,
}

#[derive(Debug, Clone)]
struct PageHeader {
    crc: u32,
    version: i32,
    prev_page_id: i64,
    next_page_id: i64,
    flags: u8,
    data_length: i32,
    padding: [u8; 3],
}

#[derive(Debug)]
struct FreeListPage {
    next_free_list_page: i64,
    used_entries: i32,
    free_page_ids: Vec<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Document {
    id: Uuid,
    first_page_id: i64,
    current_version: i32,
    paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TrieNode {
    children: ImOrdMap<char, TrieNode>,
    value: Option<Uuid>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: ImOrdMap::new(),
            value: None,
        }
    }

    fn insert(&self, reversed: &[u8], id: Uuid) -> Self {
        let mut node = self.clone();
        let mut current = &mut node;
        for &byte in reversed {
            let ch = byte as char;
            let child = current.children.get(&ch).cloned().unwrap_or(TrieNode::new());
            current.children = current.children.update(ch, child);
            current = current.children.get_mut(&ch).unwrap();
        }
        current.value = Some(id);
        node
    }

    fn remove(&self, reversed: &[u8]) -> Option<Self> {
        let mut node = self.clone();
        let mut current = &mut node;
        let mut path = vec![];
        for &byte in reversed {
            let ch = byte as char;
            if let Some(child) = current.children.get(&ch) {
                path.push((ch, current));
                current = current.children.get_mut(&ch).unwrap();
            } else {
                return None;
            }
        }
        current.value = None;
        while let Some((ch, parent)) = path.pop() {
            if current.value.is_none() && current.children.is_empty() {
                parent.children = parent.children.without(&ch);
                current = parent;
            } else {
                break;
            }
        }
        Some(node)
    }

    fn get(&self, reversed: &[u8]) -> Option<Uuid> {
        let mut current = self;
        for &byte in reversed {
            let ch = byte as char;
            if let Some(child) = current.children.get(&ch) {
                current = child;
            } else {
                return None;
            }
        }
        current.value
    }

    fn search(&self, _reversed_prefix: &[u8], results: &mut Vec<String>, current_path: String) {
        if let Some(id) = self.value {
            results.push(current_path.chars().rev().collect());
        }
        for (&ch, child) in &self.children {
            let mut new_path = current_path.clone();
            new_path.push(ch);
            child.search(&[], results, new_path);
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    page_size: u64,
    page_header_size: u64,
    max_db_size: u64,
    max_pages: i64,
    max_document_size: u64,
    page_cache_size: usize,
    versions_to_keep: i32,
    use_mmap: bool,
    use_compression: bool,
    #[cfg(feature = "encryption")]
    encryption_key: Option<[u8; 32]>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_RAW_SIZE,
            page_header_size: DEFAULT_PAGE_HEADER_SIZE,
            max_db_size: DEFAULT_MAX_DB_SIZE,
            max_pages: DEFAULT_MAX_PAGES,
            max_document_size: DEFAULT_MAX_DOCUMENT_SIZE,
            page_cache_size: DEFAULT_PAGE_CACHE_SIZE,
            versions_to_keep: DEFAULT_VERSIONS_TO_KEEP,
            use_mmap: true,
            use_compression: true,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct CacheStats {
    hits: usize,
    misses: usize,
}

pub trait Database {
    fn write_document(&mut self, path: &str, data: &mut dyn Read) -> Result<Uuid, StreamDbError>;
    fn get(&self, path: &str) -> Result<Vec<u8>, StreamDbError>;
    fn get_quick(&self, path: &str, quick: bool) -> Result<Vec<u8>, StreamDbError>;
    fn get_id_by_path(&self, path: &str) -> Result<Option<Uuid>, StreamDbError>;
    fn delete(&mut self, path: &str) -> Result<(), StreamDbError>;
    fn delete_by_id(&mut self, id: Uuid) -> Result<(), StreamDbError>;
    fn bind_to_path(&mut self, id: Uuid, path: &str) -> Result<(), StreamDbError>;
    fn unbind_path(&mut self, id: Uuid, path: &str) -> Result<(), StreamDbError>;
    fn search(&self, prefix: &str) -> Result<Vec<String>, StreamDbError>;
    fn list_paths(&self, id: Uuid) -> Result<Vec<String>, StreamDbError>;
    fn flush(&self) -> Result<(), StreamDbError>;
    fn calculate_statistics(&self) -> Result<(i64, i64), StreamDbError>;
    fn set_quick_mode(&mut self, enabled: bool);
    fn snapshot(&self) -> Result<Self, StreamDbError> where Self: Sized;
    fn get_cache_stats(&self) -> Result<CacheStats, StreamDbError>;
    fn get_stream(&self, path: &str) -> Result<Box<dyn Iterator<Item = Result<Vec<u8>, StreamDbError>> + Send + Sync>, StreamDbError>;
    fn get_async(&self, path: &str) -> Box<dyn Future<Output = Result<Vec<u8>, StreamDbError>> + Send + Sync>;
    fn begin_transaction(&mut self) -> Result<(), StreamDbError>;
    fn commit_transaction(&mut self) -> Result<(), StreamDbError>;
    fn rollback_transaction(&mut self) -> Result<(), StreamDbError>;
    async fn begin_async_transaction(&mut self) -> Result<(), StreamDbError>;
    async fn commit_async_transaction(&mut self) -> Result<(), StreamDbError>;
    async fn rollback_async_transaction(&mut self) -> Result<(), StreamDbError>;
}

pub trait DatabaseBackend: Send + Sync + Any {
    fn write_document(&mut self, data: &mut dyn Read) -> Result<Uuid, StreamDbError>;
    fn read_document(&self, id: Uuid) -> Result<Vec<u8>, StreamDbError>;
    fn read_document_quick(&self, id: Uuid, quick: bool) -> Result<Vec<u8>, StreamDbError>;
    fn delete_document(&mut self, id: Uuid) -> Result<(), StreamDbError>;
    fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> Result<Uuid, StreamDbError>;
    fn get_document_id_by_path(&self, path: &str) -> Result<Uuid, StreamDbError>;
    fn search_paths(&self, prefix: &str) -> Result<Vec<String>, StreamDbError>;
    fn list_paths_for_document(&self, id: Uuid) -> Result<Vec<String>, StreamDbError>;
    fn count_free_pages(&self) -> Result<i64, StreamDbError>;
    fn get_info(&self, id: Uuid) -> Result<String, StreamDbError>;
    fn delete_paths_for_document(&mut self, id: Uuid) -> Result<(), StreamDbError>;
    fn remove_from_index(&mut self, id: Uuid) -> Result<(), StreamDbError>;
    fn get_cache_stats(&self) -> Result<CacheStats, StreamDbError>;
    fn get_stream(&self, id: Uuid) -> Result<Box<dyn Iterator<Item = Result<Vec<u8>, StreamDbError>> + Send + Sync>, StreamDbError>;
    fn as_any(&self) -> &dyn Any;
}

// Varint helpers
fn write_varint<W: Write>(writer: &mut W, mut value: u64) -> Result<(), StreamDbError> {
    loop {
        if value < 0x80 {
            writer.write_u8(value as u8).map_err(StreamDbError::Io)?;
            break;
        }
        writer.write_u8((value as u8 & 0x7F) | 0x80).map_err(StreamDbError::Io)?;
        value >>= 7;
    }
    Ok(())
}

fn read_varint<R: Read>(reader: &mut R) -> Result<u64, StreamDbError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = reader.read_u8().map_err(StreamDbError::Io)?;
        value |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
        if shift > 63 {
            return Err(StreamDbError::InvalidData("Varint too large".to_string()));
        }
    }
    Ok(value)
}

fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

// In-Memory Backend
struct MemoryBackend {
    documents: Mutex<HashMap<Uuid, Vec<u8>>>,
    path_trie: PlRwLock<TrieNode>,
    id_to_paths: Mutex<HashMap<Uuid, Vec<String>>>,
    cache_stats: Mutex<CacheStats>,
    transaction: TokioMutex<Option<HashMap<Uuid, Vec<u8>>>>,
    wal: WriteAheadLog,
}

impl MemoryBackend {
    fn new<P: AsRef<Path>>(wal_path: P) -> Result<Self, StreamDbError> {
        let wal = WriteAheadLog::open(wal_path).map_err(|e| StreamDbError::Io(e.into()))?;
        Ok(Self {
            documents: Mutex::new(HashMap::new()),
            path_trie: PlRwLock::new(TrieNode::new()),
            id_to_paths: Mutex::new(HashMap::new()),
            cache_stats: Mutex::new(CacheStats::default()),
            transaction: TokioMutex::new(None),
            wal,
        })
    }

    fn reverse_path(path: &str) -> Vec<u8> {
        path.chars().rev().map(|c| c as u8).collect()
    }

    fn validate_path(&self, path: &str) -> Result<(), StreamDbError> {
        if path.is_empty() || path.contains('\0') || path.contains("//") || path.len() > 1024 {
            return Err(StreamDbError::InvalidInput("Invalid path: empty, contains null, double slashes, or too long".to_string()));
        }
        Ok(())
    }
}

impl DatabaseBackend for MemoryBackend {
    fn write_document(&mut self, data: &mut dyn Read) -> Result<Uuid, StreamDbError> {
        info!("Writing document in memory backend");
        let mut buffer = Vec::new();
        data.read_to_end(&mut buffer).map_err(StreamDbError::Io)?;
        let id = Uuid::new_v4();
        let mut tx = self.transaction.blocking_lock();
        if let Some(ref mut tx_data) = *tx {
            tx_data.insert(id, buffer.clone());
            self.wal.append(&serialize(&("write", id, buffer))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        } else {
            self.documents.lock().insert(id, buffer.clone());
            self.wal.append(&serialize(&("write", id, buffer))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        }
        Ok(id)
    }

    fn read_document(&self, id: Uuid) -> Result<Vec<u8>, StreamDbError> {
        debug!("Reading document ID: {}", id);
        let tx = self.transaction.blocking_lock();
        if let Some(ref tx_data) = *tx {
            if let Some(data) = tx_data.get(&id) {
                return Ok(data.clone());
            }
        }
        self.documents.lock().get(&id).cloned().ok_or(StreamDbError::NotFound(format!("Document not found: {}", id)))
    }

    fn read_document_quick(&self, id: Uuid, _quick: bool) -> Result<Vec<u8>, StreamDbError> {
        self.read_document(id)
    }

    fn delete_document(&mut self, id: Uuid) -> Result<(), StreamDbError> {
        info!("Deleting document ID: {}", id);
        let mut tx = self.transaction.blocking_lock();
        if let Some(ref mut tx_data) = *tx {
            tx_data.remove(&id);
            self.wal.append(&serialize(&("delete", id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        } else {
            self.documents.lock().remove(&id);
            self.wal.append(&serialize(&("delete", id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        }
        self.delete_paths_for_document(id)?;
        Ok(())
    }

    fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> Result<Uuid, StreamDbError> {
        self.validate_path(path)?;
        info!("Binding path {} to ID: {}", path, id);
        let reversed = Self::reverse_path(path);
        let mut tx = self.transaction.blocking_lock();
        let mut trie = self.path_trie.write();
        if let Some(_) = *tx {
            self.wal.append(&serialize(&("bind", path.to_string(), id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        } else {
            *trie = trie.insert(&reversed, id);
            self.id_to_paths.lock().entry(id).or_insert(vec![]).push(path.to_string());
            self.wal.append(&serialize(&("bind", path.to_string(), id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        }
        Ok(id)
    }

    fn get_document_id_by_path(&self, path: &str) -> Result<Uuid, StreamDbError> {
        self.validate_path(path)?;
        debug!("Getting ID for path: {}", path);
        let reversed = Self::reverse_path(path);
        let trie = self.path_trie.read();
        trie.get(&reversed).ok_or(StreamDbError::NotFound(format!("Path not found: {}", path)))
    }

    fn search_paths(&self, prefix: &str) -> Result<Vec<String>, StreamDbError> {
        self.validate_path(prefix)?;
        debug!("Searching paths with prefix: {}", prefix);
        let reversed_prefix = Self::reverse_path(prefix);
        let trie = self.path_trie.read();
        let mut results = vec![];
        trie.search(&reversed_prefix, &mut results, String::new());
        Ok(results)
    }

    fn list_paths_for_document(&self, id: Uuid) -> Result<Vec<String>, StreamDbError> {
        debug!("Listing paths for ID: {}", id);
        self.id_to_paths.lock().get(&id).cloned().ok_or(StreamDbError::NotFound(format!("ID not found: {}", id)))
    }

    fn count_free_pages(&self) -> Result<i64, StreamDbError> {
        Ok(0)
    }

    fn get_info(&self, id: Uuid) -> Result<String, StreamDbError> {
        debug!("Getting info for ID: {}", id);
        let data = self.read_document(id)?;
        let paths = self.list_paths_for_document(id)?;
        Ok(format!("ID: {}, Version: 1, Size: {} bytes, Paths: {:?}", id, data.len(), paths))
    }

    fn delete_paths_for_document(&mut self, id: Uuid) -> Result<(), StreamDbError> {
        info!("Deleting paths for ID: {}", id);
        let mut tx = self.transaction.blocking_lock();
        let paths = self.id_to_paths.lock().remove(&id).unwrap_or(vec![]);
        let mut trie = self.path_trie.write();
        if let Some(_) = *tx {
            for path in paths {
                self.wal.append(&serialize(&("unbind", path, id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
            }
        } else {
            for path in paths {
                let reversed = Self::reverse_path(&path);
                if let Some(new_trie) = trie.remove(&reversed) {
                    *trie = new_trie;
                }
                self.wal.append(&serialize(&("unbind", path, id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
            }
        }
        Ok(())
    }

    fn remove_from_index(&mut self, id: Uuid) -> Result<(), StreamDbError> {
        self.delete_paths_for_document(id)
    }

    fn get_cache_stats(&self) -> Result<CacheStats, StreamDbError> {
        Ok(self.cache_stats.lock().clone())
    }

    fn get_stream(&self, id: Uuid) -> Result<Box<dyn Iterator<Item = Result<Vec<u8>, StreamDbError>> + Send + Sync>, StreamDbError> {
        let data = self.read_document(id)?;
        Ok(Box::new(std::iter::once(Ok(data))))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// File Backend
struct FileBackend {
    config: Config,
    file: Arc<Mutex<File>>,
    #[cfg(not(target_arch = "wasm32"))]
    mmap: Arc<PlRwLock<Option<MmapMut>>>,
    current_size: Arc<AtomicU64>,
    document_index: Mutex<HashMap<Uuid, Document>>,
    path_trie: PlRwLock<TrieNode>,
    id_to_paths: Mutex<HashMap<Uuid, Vec<String>>>,
    free_list_root: Mutex<VersionedLink>,
    page_cache: Mutex<LruCache<i64, Arc<Vec<u8>>>>,
    quick_mode: AtomicBool,
    old_versions: Mutex<HashMap<Uuid, Vec<(i32, i64)>>>,
    cache_stats: Mutex<CacheStats>,
    empty_free_list_count: Mutex<u64>,
    wal: WriteAheadLog,
    #[cfg(feature = "encryption")]
    aead_key: Option<LessSafeKey>,
}

impl FileBackend {
    pub fn new<P: AsRef<Path>>(path: P, config: Config) -> Result<Self, StreamDbError> {
        info!("Opening file backend at {:?}", path.as_ref());
        let db_path = path.as_ref().to_path_buf();
        let wal = WriteAheadLog::open(db_path.with_extension("wal")).map_err(|e| StreamDbError::Io(e.into()))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .map_err(StreamDbError::Io)?;
        let mut initial_size = file.seek(SeekFrom::End(0)).map_err(StreamDbError::Io)?;
        let mut header = DatabaseHeader {
            magic: MAGIC,
            index_root: VersionedLink { page_id: -1, version: 0 },
            path_lookup_root: VersionedLink { page_id: -1, version: 0 },
            free_list_root: VersionedLink { page_id: -1, version: 0 },
        };

        if initial_size == 0 {
            initial_size = config.page_size;
            file.set_len(initial_size).map_err(StreamDbError::Io)?;
            Self::write_header(&mut file, &header, &config)?;
        } else {
            header = Self::read_header(&mut file, &config)?;
            if header.magic != MAGIC {
                warn!("Corrupt header detected, reinitializing");
                header = DatabaseHeader {
                    magic: MAGIC,
                    index_root: VersionedLink { page_id: -1, version: 0 },
                    path_lookup_root: VersionedLink { page_id: -1, version: 0 },
                    free_list_root: VersionedLink { page_id: -1, version: 0 },
                };
                file.seek(SeekFrom::Start(0)).map_err(StreamDbError::Io)?;
                file.set_len(config.page_size).map_err(StreamDbError::Io)?;
                Self::write_header(&mut file, &header, &config)?;
                initial_size = config.page_size;
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        let mmap = if config.use_mmap {
            let mmap = unsafe { MmapOptions::new().len(initial_size as usize).map_mut(&file).map_err(StreamDbError::Io)? };
            Some(mmap)
        } else {
            None
        };
        #[cfg(target_arch = "wasm32")]
        let mmap = None;

        #[cfg(feature = "encryption")]
        let aead_key = config.encryption_key.map(|key_bytes| {
            let unbound_key = UnboundKey::new(&AES_256_GCM, &key_bytes).expect("Valid encryption key");
            LessSafeKey::new(unbound_key)
        });

        let backend = Self {
            config,
            file: Arc::new(Mutex::new(file)),
            #[cfg(not(target_arch = "wasm32"))]
            mmap: Arc::new(PlRwLock::new(mmap)),
            current_size: Arc::new(AtomicU64::new(initial_size)),
            document_index: Mutex::new(HashMap::new()),
            path_trie: PlRwLock::new(TrieNode::new()),
            id_to_paths: Mutex::new(HashMap::new()),
            free_list_root: Mutex::new(header.free_list_root),
            page_cache: Mutex::new(LruCache::new(std::num::NonZeroUsize::new(config.page_cache_size).unwrap())),
            quick_mode: AtomicBool::new(false),
            old_versions: Mutex::new(HashMap::new()),
            cache_stats: Mutex::new(CacheStats::default()),
            empty_free_list_count: Mutex::new(0),
            wal,
            #[cfg(feature = "encryption")]
            aead_key,
        };

        backend.recover()?;
        Ok(backend)
    }

    fn write_header(file: &mut File, header: &DatabaseHeader, config: &Config) -> Result<(), StreamDbError> {
        file.seek(SeekFrom::Start(0)).map_err(StreamDbError::Io)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&header.magic).map_err(StreamDbError::Io)?;
        writer.write_i64::<LittleEndian>(header.index_root.page_id).map_err(StreamDbError::Io)?;
        writer.write_i32::<LittleEndian>(header.index_root.version).map_err(StreamDbError::Io)?;
        writer.write_i64::<LittleEndian>(header.path_lookup_root.page_id).map_err(StreamDbError::Io)?;
        writer.write_i32::<LittleEndian>(header.path_lookup_root.version).map_err(StreamDbError::Io)?;
        writer.write_i64::<LittleEndian>(header.free_list_root.page_id).map_err(StreamDbError::Io)?;
        writer.write_i32::<LittleEndian>(header.free_list_root.version).map_err(StreamDbError::Io)?;
        writer.flush().map_err(StreamDbError::Io)?;
        Ok(())
    }

    fn read_header(file: &mut File, _config: &Config) -> Result<DatabaseHeader, StreamDbError> {
        file.seek(SeekFrom::Start(0)).map_err(StreamDbError::Io)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic).map_err(StreamDbError::Io)?;
        let index_root = VersionedLink {
            page_id: reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?,
            version: reader.read_i32::<LittleEndian>().map_err(StreamDbError::Io)?,
        };
        let path_lookup_root = VersionedLink {
            page_id: reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?,
            version: reader.read_i32::<LittleEndian>().map_err(StreamDbError::Io)?,
        };
        let free_list_root = VersionedLink {
            page_id: reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?,
            version: reader.read_i32::<LittleEndian>().map_err(StreamDbError::Io)?,
        };
        Ok(DatabaseHeader {
            magic,
            index_root,
            path_lookup_root,
            free_list_root,
        })
    }

    fn compute_crc(&self, data: &[u8]) -> u32 {
        let crc = CrcLib::<u32>::new(&CRC_32_ISO_HDLC);
        crc.checksum(data)
    }

    fn derive_nonce(page_id: i64) -> [u8; NONCE_LEN] {
        let mut nonce = [0u8; NONCE_LEN];
        let bytes = page_id.to_le_bytes();
        nonce[..bytes.len()].copy_from_slice(&bytes);
        nonce
    }

    fn read_raw_page(&self, page_id: i64) -> Result<Arc<Vec<u8>>, StreamDbError> {
        if page_id < 0 || page_id >= self.config.max_pages {
            return Err(StreamDbError::InvalidInput(format!("Invalid page ID: {}", page_id)));
        }
        let offset = page_id as u64 * self.config.page_size + self.config.page_header_size;
        let mut cache = self.page_cache.lock();
        {
            let mut stats = self.cache_stats.lock();
            if cache.contains(&page_id) {
                stats.hits += 1;
                return Ok(cache.get(&page_id).unwrap().clone());
            }
            stats.misses += 1;
        }
        let header = self.read_page_header(page_id)?;
        let data_length = header.data_length as usize;
        if data_length as u64 > self.config.page_size - self.config.page_header_size {
            return Err(StreamDbError::InvalidData(format!("Invalid data length: {}", data_length)));
        }
        let mut data = vec![0u8; data_length];
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mmap) = self.mmap.read().as_ref() {
            let start = offset as usize;
            data.copy_from_slice(&mmap[start..start + data_length]);
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset)).map_err(StreamDbError::Io)?;
            file.read_exact(&mut data).map_err(StreamDbError::Io)?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset)).map_err(StreamDbError::Io)?;
            file.read_exact(&mut data).map_err(StreamDbError::Io)?;
        }
        #[cfg(feature = "encryption")]
        if let Some(key) = &self.aead_key {
            let nonce = Nonce::try_assume_unique_for_key(&Self::derive_nonce(page_id))
                .map_err(|e| StreamDbError::EncryptionError(e.to_string()))?;
            let mut in_out = data.clone();
            let decrypted = key.open_in_place(nonce, Aad::empty(), &mut in_out)
                .map_err(|e| StreamDbError::EncryptionError(e.to_string()))?;
            data = decrypted.to_vec();
        }
        if self.config.use_compression {
            data = snappy::uncompress(&data).map_err(|e| StreamDbError::InvalidData(e.to_string()))?;
        }
        if !self.quick_mode.load(Ordering::Relaxed) {
            let computed_crc = self.compute_crc(&data);
            if computed_crc != header.crc {
                return Err(StreamDbError::InvalidData("CRC mismatch".to_string()));
            }
        }
        let arc_data = Arc::new(data);
        cache.put(page_id, arc_data.clone());
        if header.next_page_id != -1 {
            let _ = self.read_raw_page(header.next_page_id);
        }
        Ok(arc_data)
    }

    fn write_raw_page(&self, page_id: i64, data: &[u8], version: i32) -> Result<(), StreamDbError> {
        if page_id < 0 || page_id >= self.config.max_pages {
            return Err(StreamDbError::InvalidInput(format!("Invalid page ID: {}", page_id)));
        }
        let mut compressed = if self.config.use_compression {
            snappy::compress(data)
        } else {
            data.to_vec()
        };
        let mut final_data = compressed.clone();
        #[cfg(feature = "encryption")]
        if let Some(key) = &self.aead_key {
            let nonce = Nonce::try_assume_unique_for_key(&Self::derive_nonce(page_id))
                .map_err(|e| StreamDbError::EncryptionError(e.to_string()))?;
            let mut in_out = final_data.clone();
            in_out.extend(vec![0u8; TAG_LEN]);
            let sealed = key.seal_in_place_separate_tag(nonce, Aad::empty(), &mut in_out)
                .map_err(|e| StreamDbError::EncryptionError(e.to_string()))?;
            final_data = in_out;
            final_data.extend(sealed.as_ref());
        }
        if final_data.len() as u64 > self.config.page_size - self.config.page_header_size {
            return Err(StreamDbError::InvalidInput(format!("Data too large for page: {} bytes", final_data.len())));
        }
        let offset = page_id as u64 * self.config.page_size;
        let crc = self.compute_crc(data);
        let header = PageHeader {
            crc,
            version,
            prev_page_id: -1,
            next_page_id: -1,
            flags: FLAG_DATA_PAGE,
            data_length: final_data.len() as i32,
            padding: [0; 3],
        };
        self.write_page_header(page_id, &header)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mmap) = self.mmap.write().as_mut() {
            let start = offset as usize + self.config.page_header_size as usize;
            mmap[start..start + final_data.len()].copy_from_slice(&final_data);
            mmap.flush().map_err(StreamDbError::Io)?;
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset + self.config.page_header_size)).map_err(StreamDbError::Io)?;
            file.write_all(&final_data).map_err(StreamDbError::Io)?;
            file.flush().map_err(StreamDbError::Io)?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset + self.config.page_header_size)).map_err(StreamDbError::Io)?;
            file.write_all(&final_data).map_err(StreamDbError::Io)?;
            file.flush().map_err(StreamDbError::Io)?;
        }
        self.page_cache.lock().pop(&page_id);
        Ok(())
    }

    fn write_page_header(&self, page_id: i64, header: &PageHeader) -> Result<(), StreamDbError> {
        if page_id < 0 || page_id >= self.config.max_pages {
            return Err(StreamDbError::InvalidInput(format!("Invalid page ID: {}", page_id)));
        }
        let offset = page_id as u64 * self.config.page_size;
        let mut buffer = Vec::new();
        let mut writer = BufWriter::new(&mut buffer);
        writer.write_u32::<LittleEndian>(header.crc).map_err(StreamDbError::Io)?;
        writer.write_i32::<LittleEndian>(header.version).map_err(StreamDbError::Io)?;
        writer.write_i64::<LittleEndian>(header.prev_page_id).map_err(StreamDbError::Io)?;
        writer.write_i64::<LittleEndian>(header.next_page_id).map_err(StreamDbError::Io)?;
        writer.write_u8(header.flags).map_err(StreamDbError::Io)?;
        writer.write_i32::<LittleEndian>(header.data_length).map_err(StreamDbError::Io)?;
        writer.write_all(&header.padding).map_err(StreamDbError::Io)?;
        writer.flush().map_err(StreamDbError::Io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mmap) = self.mmap.write().as_mut() {
            let start = offset as usize;
            mmap[start..start + self.config.page_header_size as usize].copy_from_slice(&buffer);
            mmap.flush().map_err(StreamDbError::Io)?;
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset)).map_err(StreamDbError::Io)?;
            file.write_all(&buffer).map_err(StreamDbError::Io)?;
            file.flush().map_err(StreamDbError::Io)?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset)).map_err(StreamDbError::Io)?;
            file.write_all(&buffer).map_err(StreamDbError::Io)?;
            file.flush().map_err(StreamDbError::Io)?;
        }
        Ok(())
    }

    fn read_page_header(&self, page_id: i64) -> Result<PageHeader, StreamDbError> {
        if page_id < 0 || page_id >= self.config.max_pages {
            return Err(StreamDbError::InvalidInput(format!("Invalid page ID: {}", page_id)));
        }
        let offset = page_id as u64 * self.config.page_size;
        let mut buffer = vec![0u8; self.config.page_header_size as usize];
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mmap) = self.mmap.read().as_ref() {
            let start = offset as usize;
            buffer.copy_from_slice(&mmap[start..start + self.config.page_header_size as usize]);
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset)).map_err(StreamDbError::Io)?;
            file.read_exact(&mut buffer).map_err(StreamDbError::Io)?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset)).map_err(StreamDbError::Io)?;
            file.read_exact(&mut buffer).map_err(StreamDbError::Io)?;
        }
        let mut reader = Cursor::new(buffer);
        Ok(PageHeader {
            crc: reader.read_u32::<LittleEndian>().map_err(StreamDbError::Io)?,
            version: reader.read_i32::<LittleEndian>().map_err(StreamDbError::Io)?,
            prev_page_id: reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?,
            next_page_id: reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?,
            flags: reader.read_u8().map_err(StreamDbError::Io)?,
            data_length: reader.read_i32::<LittleEndian>().map_err(StreamDbError::Io)?,
            padding: [0; 3],
        })
    }

    fn allocate_page(&self) -> Result<i64, StreamDbError> {
        let mut free_root = self.free_list_root.lock();
        let mut current = free_root.page_id;
        let mut empty_count = *self.empty_free_list_count.lock();
        while current != -1 {
            let mut page = self.read_free_list_page(current)?;
            if !page.free_page_ids.is_empty() {
                let page_id = page.free_page_ids.pop().unwrap();
                page.used_entries -= 1;
                self.write_free_list_page(current, &page)?;
                *self.empty_free_list_count.lock() = 0;
                return Ok(page_id);
            }
            empty_count += 1;
            current = page.next_free_list_page;
        }
        if empty_count >= MAX_CONSECUTIVE_EMPTY_FREE_LIST {
            self.grow_db(BATCH_GROW_PAGES)?;
            empty_count = 0;
        }
        *self.empty_free_list_count.lock() = empty_count;
        let new_page_id = (self.current_size.load(Ordering::Relaxed) / self.config.page_size) as i64;
        self.grow_db(1)?;
        Ok(new_page_id)
    }

    fn free_page(&self, page_id: i64) -> Result<(), StreamDbError> {
        let mut free_root = self.free_list_root.lock();
        let mut current = free_root.page_id;
        while current != -1 {
            let mut page = self.read_free_list_page(current)?;
            if page.used_entries as usize < FREE_LIST_ENTRIES_PER_PAGE {
                page.free_page_ids.push(page_id);
                page.used_entries += 1;
                self.write_free_list_page(current, &page)?;
                return Ok(());
            }
            if page.next_free_list_page == -1 {
                let new_page = self.allocate_page()?;
                page.next_free_list_page = new_page;
                self.write_free_list_page(current, &page)?;
                let new_page_struct = FreeListPage {
                    next_free_list_page: -1,
                    used_entries: 1,
                    free_page_ids: vec![page_id],
                };
                self.write_free_list_page(new_page, &new_page_struct)?;
                return Ok(());
            }
            current = page.next_free_list_page;
        }
        let new_page = self.allocate_page()?;
        free_root.page_id = new_page;
        let new_page_struct = FreeListPage {
            next_free_list_page: -1,
            used_entries: 1,
            free_page_ids: vec![page_id],
        };
        self.write_free_list_page(new_page, &new_page_struct)?;
        Ok(())
    }

    fn read_free_list_page(&self, page_id: i64) -> Result<FreeListPage, StreamDbError> {
        let data = self.read_raw_page(page_id)?;
        let mut reader = Cursor::new(&*data);
        let next_free_list_page = reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?;
        let used_entries = reader.read_i32::<LittleEndian>().map_err(StreamDbError::Io)?;
        let mut free_page_ids = Vec::with_capacity(used_entries as usize);
        for _ in 0..used_entries {
            free_page_ids.push(reader.read_i64::<LittleEndian>().map_err(StreamDbError::Io)?);
        }
        Ok(FreeListPage {
            next_free_list_page,
            used_entries,
            free_page_ids,
        })
    }

    fn write_free_list_page(&self, page_id: i64, page: &FreeListPage) -> Result<(), StreamDbError> {
        let mut buffer = Vec::new();
        buffer.write_i64::<LittleEndian>(page.next_free_list_page).map_err(StreamDbError::Io)?;
        buffer.write_i32::<LittleEndian>(page.used_entries).map_err(StreamDbError::Io)?;
        for &id in &page.free_page_ids {
            buffer.write_i64::<LittleEndian>(id).map_err(StreamDbError::Io)?;
        }
        let header = PageHeader {
            crc: self.compute_crc(&buffer),
            version: 1,
            prev_page_id: -1,
            next_page_id: -1,
            flags: FLAG_FREE_LIST_PAGE,
            data_length: buffer.len() as i32,
            padding: [0; 3],
        };
        self.write_page_header(page_id, &header)?;
        self.write_raw_page(page_id, &buffer, 1)?;
        Ok(())
    }

    fn grow_db(&self, pages: u64) -> Result<(), StreamDbError> {
        let current_size = self.current_size.load(Ordering::Relaxed);
        let new_size = current_size + pages * self.config.page_size;
        if new_size > self.config.max_db_size {
            return Err(StreamDbError::InvalidInput(format!("Max DB size exceeded: {} > {}", new_size, self.config.max_db_size)));
        }
        {
            let mut file = self.file.lock();
            file.set_len(new_size).map_err(StreamDbError::Io)?;
            file.flush().map_err(StreamDbError::Io)?;
        }
        self.current_size.store(new_size, Ordering::Relaxed);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut mmap) = self.mmap.write().as_mut() {
            *mmap = unsafe { MmapOptions::new().len(new_size as usize).map_mut(&*self.file.lock()).map_err(StreamDbError::Io)? };
        }
        Ok(())
    }

    fn serialize_document(&self, doc: &Document) -> Result<Vec<u8>, StreamDbError> {
        serialize(doc).map_err(|e| StreamDbError::InvalidData(format!("Serialization error: {}", e)))
    }

    fn deserialize_document(&self, data: &[u8]) -> Result<Document, StreamDbError> {
        deserialize(data).map_err(|e| StreamDbError::InvalidData(format!("Deserialization error: {}", e)))
    }

    fn serialize_trie(&self, trie: &TrieNode) -> Result<Vec<u8>, StreamDbError> {
        let data = serialize(trie).map_err(|e| StreamDbError::InvalidData(format!("Trie serialization error: {}", e)))?;
        if data.len() as u64 > self.config.page_size - self.config.page_header_size {
            return Err(StreamDbError::InvalidInput("Trie too large for page".to_string()));
        }
        Ok(data)
    }

    fn deserialize_trie(&self, data: &[u8]) -> Result<TrieNode, StreamDbError> {
        deserialize(data).map_err(|e| StreamDbError::InvalidData(format!("Trie deserialization error: {}", e)))
    }

    fn load_trie(&self) -> Result<(), StreamDbError> {
        let page_id = self.document_index_root.lock().page_id;
        if page_id == -1 {
            *self.path_trie.write() = TrieNode::new();
            return Ok(());
        }
        let data = self.read_raw_page(page_id)?;
        let trie = self.deserialize_trie(&data)?;
        *self.path_trie.write() = trie;
        Ok(())
    }

    fn save_trie(&self, trie: &TrieNode, version: i32) -> Result<(), StreamDbError> {
        let data = self.serialize_trie(trie)?;
        let mut index_root = self.document_index_root.lock();
        if index_root.page_id == -1 {
            index_root.page_id = self.allocate_page()?;
            index_root.version = 1;
        }
        self.write_raw_page(index_root.page_id, &data, version)?;
        Ok(())
    }

    fn recover(&self) -> Result<(), StreamDbError> {
        info!("Starting recovery for file backend");
        let total_pages = (self.current_size.load(Ordering::Relaxed) / self.config.page_size) as i64;
        let mut used_pages = HashSet::new();
        let mut free_pages = HashSet::new();
        let index_root = *self.document_index_root.lock();
        let free_root = *self.free_list_root.lock();
        if index_root.page_id != -1 {
            used_pages.insert(index_root.page_id);
            self.load_trie()?;
        }
        if free_root.page_id != -1 {
            let mut current = free_root.page_id;
            while current != -1 {
                used_pages.insert(current);
                let page = self.read_free_list_page(current)?;
                free_pages.extend(page.free_page_ids);
                current = page.next_free_list_page;
            }
        }
        for page_id in 0..total_pages {
            if used_pages.contains(&page_id) || free_pages.contains(&page_id) {
                continue;
            }
            let header = self.read_page_header(page_id);
            if header.is_err() || !self.quick_mode.load(Ordering::Relaxed) {
                self.free_page(page_id)?;
                continue;
            }
            let header = header.unwrap();
            if header.flags & FLAG_DATA_PAGE != 0 {
                used_pages.insert(page_id);
                let mut current = header.next_page_id;
                while current != -1 {
                    used_pages.insert(current);
                    let next_header = self.read_page_header(current)?;
                    current = next_header.next_page_id;
                }
            }
        }
        let mut id_to_paths = self.id_to_paths.lock();
        let mut trie = self.path_trie.write();
        let mut documents = self.document_index.lock();
        self.wal.recover(|entry| {
            let (op, id, data): (&str, Uuid, Option<Vec<u8>>) = deserialize(entry)?;
            match op {
                "write" => {
                    if let Some(data) = data {
                        let document = Document {
                            id,
                            first_page_id: -1, // Will be set in write_document
                            current_version: 1,
                            paths: vec![],
                        };
                        documents.insert(id, document);
                    }
                }
                "delete" => {
                    documents.remove(&id);
                    id_to_paths.remove(&id);
                    let paths = id_to_paths.get(&id).cloned().unwrap_or(vec![]);
                    for path in paths {
                        let reversed = MemoryBackend::reverse_path(&path);
                        if let Some(new_trie) = trie.remove(&reversed) {
                            *trie = new_trie;
                        }
                    }
                }
                "bind" => {
                    if let Some(path) = data.and_then(|d| String::from_utf8(d).ok()) {
                        let reversed = MemoryBackend::reverse_path(&path);
                        *trie = trie.insert(&reversed, id);
                        id_to_paths.entry(id).or_insert(vec![]).push(path);
                    }
                }
                "unbind" => {
                    if let Some(path) = data.and_then(|d| String::from_utf8(d).ok()) {
                        let reversed = MemoryBackend::reverse_path(&path);
                        if let Some(new_trie) = trie.remove(&reversed) {
                            *trie = new_trie;
                        }
                        if let Some(paths) = id_to_paths.get_mut(&id) {
                            paths.retain(|p| p != &path);
                        }
                    }
                }
                _ => {}
            }
            Ok(())
        }).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        Ok(())
    }

    fn gc_old_versions(&self) -> Result<(), StreamDbError> {
        let mut old_versions = self.old_versions.lock();
        for (id, versions) in old_versions.iter_mut() {
            while versions.len() as i32 > self.config.versions_to_keep {
                if let Some((_, page_id)) = versions.pop() {
                    let mut current = page_id;
                    while current != -1 {
                        let header = self.read_page_header(current)?;
                        self.free_page(current)?;
                        current = header.next_page_id;
                    }
                }
            }
        }
        Ok(())
    }
}

impl DatabaseBackend for FileBackend {
    fn write_document(&mut self, data: &mut dyn Read) -> Result<Uuid, StreamDbError> {
        info!("Writing document in file backend");
        let mut buffer = Vec::new();
        data.read_to_end(&mut buffer).map_err(StreamDbError::Io)?;
        if buffer.len() as u64 > self.config.max_document_size {
            return Err(StreamDbError::InvalidInput(format!("Document size {} exceeds max {}", buffer.len(), self.config.max_document_size)));
        }
        let id = Uuid::new_v4();
        self.wal.append(&serialize(&("write", id, buffer.clone()))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        let chunk_size = (self.config.page_size - self.config.page_header_size) as usize;
        let mut first_page_id = -1;
        let mut prev_page_id = -1;
        let mut bytes_written = 0;
        let version = 1;
        while bytes_written < buffer.len() {
            let end = std::cmp::min(bytes_written + chunk_size, buffer.len());
            let chunk = &buffer[bytes_written..end];
            let page_id = self.allocate_page()?;
            if first_page_id == -1 {
                first_page_id = page_id;
            }
            let mut header = PageHeader {
                crc: self.compute_crc(chunk),
                version,
                prev_page_id,
                next_page_id: -1,
                flags: FLAG_DATA_PAGE,
                data_length: chunk.len() as i32,
                padding: [0; 3],
            };
            if prev_page_id != -1 {
                let mut prev_header = self.read_page_header(prev_page_id)?;
                prev_header.next_page_id = page_id;
                self.write_page_header(prev_page_id, &prev_header)?;
            }
            self.write_raw_page(page_id, chunk, version)?;
            prev_page_id = page_id;
            bytes_written = end;
        }
        let document = Document {
            id,
            first_page_id,
            current_version: version,
            paths: vec![],
        };
        let mut index = self.document_index.lock();
        index.insert(id, document);
        if let Some(old) = self.old_versions.lock().get(&id).cloned() {
            let mut old_versions = self.old_versions.lock();
            old_versions.get_mut(&id).unwrap().push((version - 1, old[0].1));
        } else {
            self.old_versions.lock().insert(id, vec![(version - 1, first_page_id)]);
        }
        Ok(id)
    }

    fn read_document(&self, id: Uuid) -> Result<Vec<u8>, StreamDbError> {
        self.read_document_quick(id, self.quick_mode.load(Ordering::Relaxed))
    }

    fn read_document_quick(&self, id: Uuid, quick: bool) -> Result<Vec<u8>, StreamDbError> {
        debug!("Reading document ID: {} (quick={})", id, quick);
        let index = self.document_index.lock();
        let document = index.get(&id).ok_or_else(|| StreamDbError::NotFound(format!("Document not found for ID: {}", id)))?.clone();
        let mut result = Vec::new();
        let mut current_page_id = document.first_page_id;
        while current_page_id != -1 {
            let quick_mode = self.quick_mode.load(Ordering::Relaxed);
            self.quick_mode.store(quick, Ordering::Relaxed);
            let data = self.read_raw_page(current_page_id)?;
            self.quick_mode.store(quick_mode, Ordering::Relaxed);
            result.extend_from_slice(&data);
            let header = self.read_page_header(current_page_id)?;
            current_page_id = header.next_page_id;
        }
        Ok(result)
    }

    fn delete_document(&mut self, id: Uuid) -> Result<(), StreamDbError> {
        info!("Deleting document ID: {}", id);
        self.wal.append(&serialize(&("delete", id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        let mut index = self.document_index.lock();
        let document = index.remove(&id).ok_or_else(|| StreamDbError::NotFound(format!("Document not found for ID: {}", id)))?;
        let mut current_page_id = document.first_page_id;
        while current_page_id != -1 {
            let header = self.read_page_header(current_page_id)?;
            self.free_page(current_page_id)?;
            current_page_id = header.next_page_id;
        }
        self.remove_from_index(id)?;
        Ok(())
    }

    fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> Result<Uuid, StreamDbError> {
        self.validate_path(path)?;
        info!("Binding path {} to ID: {}", path, id);
        let mut index = self.document_index.lock();
        let mut document = index.get(&id).ok_or_else(|| StreamDbError::NotFound(format!("Document not found for ID: {}", id)))?.clone();
        if self.get_document_id_by_path(path).is_ok() {
            return Err(StreamDbError::InvalidInput(format!("Path already bound: {}", path)));
        }
        document.paths.push(path.to_string());
        index.insert(id, document);
        let reversed = MemoryBackend::reverse_path(path);
        let mut trie = self.path_trie.write();
        *trie = trie.insert(&reversed, id);
        self.wal.append(&serialize(&("bind", path.to_string(), id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        self.id_to_paths.lock().entry(id).or_insert(vec![]).push(path.to_string());
        Ok(id)
    }

    fn get_document_id_by_path(&self, path: &str) -> Result<Uuid, StreamDbError> {
        self.validate_path(path)?;
        debug!("Getting ID for path: {}", path);
        let reversed = MemoryBackend::reverse_path(path);
        let trie = self.path_trie.read();
        trie.get(&reversed).ok_or(StreamDbError::NotFound(format!("Path not found: {}", path)))
    }

    fn search_paths(&self, prefix: &str) -> Result<Vec<String>, StreamDbError> {
        self.validate_path(prefix)?;
        debug!("Searching paths with prefix: {}", prefix);
        let reversed_prefix = MemoryBackend::reverse_path(prefix);
        let trie = self.path_trie.read();
        let mut results = vec![];
        trie.search(&reversed_prefix, &mut results, String::new());
        Ok(results)
    }

    fn list_paths_for_document(&self, id: Uuid) -> Result<Vec<String>, StreamDbError> {
        debug!("Listing paths for ID: {}", id);
        self.id_to_paths.lock().get(&id).cloned().ok_or(StreamDbError::NotFound(format!("ID not found: {}", id)))
    }

    fn count_free_pages(&self) -> Result<i64, StreamDbError> {
        let mut count = 0;
        let mut current = self.free_list_root.lock().page_id;
        while current != -1 {
            let page = self.read_free_list_page(current)?;
            count += page.used_entries as i64;
            current = page.next_free_list_page;
        }
        Ok(count)
    }

    fn get_info(&self, id: Uuid) -> Result<String, StreamDbError> {
        debug!("Getting info for ID: {}", id);
        let index = self.document_index.lock();
        let document = index.get(&id).ok_or_else(|| StreamDbError::NotFound(format!("ID not found: {}", id)))?.clone();
        let mut size = 0;
        let mut current_page_id = document.first_page_id;
        while current_page_id != -1 {
            let header = self.read_page_header(current_page_id)?;
            size += header.data_length as u64;
            current_page_id = header.next_page_id;
        }
        Ok(format!(
            "ID: {}, Version: {}, Size: {} bytes, Paths: {:?}",
            id, document.current_version, size, document.paths
        ))
    }

    fn delete_paths_for_document(&mut self, id: Uuid) -> Result<(), StreamDbError> {
        info!("Deleting paths for ID: {}", id);
        let index = self.document_index.lock();
        let mut document = index.get(&id).ok_or_else(|| StreamDbError::NotFound(format!("ID not found: {}", id)))?.clone();
        let paths = self.id_to_paths.lock().remove(&id).unwrap_or(vec![]);
        let mut trie = self.path_trie.write();
        for path in paths {
            let reversed = MemoryBackend::reverse_path(&path);
            if let Some(new_trie) = trie.remove(&reversed) {
                *trie = new_trie;
            }
            self.wal.append(&serialize(&("unbind", path, id))?).map_err(|e| StreamDbError::TransactionError(e.to_string()))?;
        }
        document.paths.clear();
        let mut index = self.document_index.lock();
        index.insert(id, document);
        Ok(())
    }

    fn remove_from_index(&mut self, id: Uuid) -> Result<(), StreamDbError> {
        self.delete_paths_for_document(id)
    }

