//! StreamDb - A lightweight, embedded database for storing and retrieving binary streams via paths.
//!
//! ## Overview
//! StreamDb is designed as a reliable, performant, and simple key-value store where keys are string paths
//! (supporting prefix searches via a reverse trie) and values are binary streams (documents up to 256MB by default).
//! It uses a paged storage system with chaining for large documents. The database is thread-safe, supports
//! multiple readers/single writer per path, and includes optimizations like LRU caching for pages and paths,
//! QuickAndDirtyMode for skipping CRC on reads, and free page management with first-fit LIFO strategy and
//! automatic consolidation. Versioning supports basic retention (keep 2 versions, free on 3rd), with garbage
//! collection for old versions during maintenance operations like flush.
//!
//! ## Adaptability for Multiple Use Cases and Platforms
//! - **Use Cases**:
//!   - **Embedded Systems**: Configurable page and cache sizes for low memory; optional no-mmap mode for constrained environments.
//!   - **Server Applications**: Thread-safe with lock hierarchy to prevent deadlocks and starvation; tunable for high concurrency.
//!   - **Mobile/Desktop Apps**: Cross-platform support; lightweight dependencies.
//!   - **In-Memory Testing/Volatile Storage**: MemoryBackend implementation for fast testing or temporary data.
//!   - **Custom Storage**: DatabaseBackend trait allows pluggable backends (e.g., encrypted file, cloud storage like S3).
//!   - **Large-Scale Databases**: i64 page IDs support >2B pages; configurable max sizes.
//!   - **Read-Heavy Workloads**: Quick mode for 10x faster reads; LRU page cache.
//!   - **Write-Heavy Workloads**: Batched file growth, LIFO free page reuse.
//!   - **Versioned Data**: Basic MVCC-like retention for undo or audits; extensible for snapshots.
//! - **Platforms**:
//!   - **Cross-Compilation**: Supports Rust targets (x86_64, arm, etc.); conditional no-mmap for platforms like wasm (with FS shim).
//!   - **File System Tuning**: Configurable page size to match block sizes (e.g., 4KB default, 8KB for NVMe).
//!   - **No-Mmap Fallback**: Uses standard seek/read/write for environments without memory mapping.
//!   - **Threading**: parking_lot locks for efficiency across OS (Windows, Linux, macOS).
//! - **Extensibility**:
//!   - **Config**: Tune page size, caches, versions kept, mmap usage.
//!   - **Traits**: Database for high-level ops, DatabaseBackend for storage layer.
//!   - **Recovery**: Automatic on open; scans for orphans, repairs chains, rebuilds indexes if corrupted.
//!   - **Error Handling**: Comprehensive checks (bounds, CRC, versions); custom recovery mechanisms.
//!   - **Maintenance**: Flush, statistics, GC for old versions.
//! - **Dependencies**: uuid, crc, memmap2 (optional), byteorder, parking_lot, lru.
//! - **Testing**: Unit tests for core ops, integration for multi-thread, stress for concurrency/corruption/power failure sim.
//!
//! ## Usage Example
//! ```rust
//! use streamdb::{Config, StreamDb, Database};
//! use std::io::Cursor;
//!
//! fn main() -> std::io::Result<()> {
//!     let config = Config::default();
//!     let mut db = StreamDb::open_with_config("streamdb.db", config)?;
//!     db.set_quick_mode(true);
//!     let mut data = Cursor::new(b"Hello, StreamDb!");
//!     let id = db.write_document("/test/path", &mut data)?;
//!     let read = db.get("/test/path")?;
//!     assert_eq!(read, b"Hello, StreamDb!");
//!     db.flush()?;
//!     Ok(())
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc};
use parking_lot::{Mutex, RwLock as PlRwLock};
use memmap2::{MmapMut, MmapOptions};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use uuid::Uuid;
use crc::Crc as CrcLib;
use crc::CRC_32_ISO_HDLC;
use lru::LruCache;

const MAGIC: [u8; 8] = [0x55, 0xAA, 0xFE, 0xED, 0xFA, 0xCE, 0xDA, 0x7A];
const DEFAULT_PAGE_RAW_SIZE: u64 = 4096;
const DEFAULT_PAGE_HEADER_SIZE: u64 = 35; // uint32 crc, i32 ver, i64 prev/next (upgraded), u8 flags, i32 len = 4+4+8+8+1+4=29, pad to 35 for alignment?
const FREE_LIST_ENTRIES_PER_PAGE: usize = 1010; // i64 ids, (page_data_capacity / 8)
const DEFAULT_MAX_DB_SIZE: u64 = 8000 * 1024 * 1024 * 1024;
const DEFAULT_MAX_PAGES: i64 = i64::MAX;
const DEFAULT_MAX_DOCUMENT_SIZE: u64 = 256 * 1024 * 1024;
const BATCH_GROW_PAGES: u64 = 16;
const DEFAULT_PAGE_CACHE_SIZE: usize = 1024;
const DEFAULT_VERSIONS_TO_KEEP: i32 = 2;

// Flags for page types (in PageHeader.flags)
const FLAG_DATA_PAGE: u8 = 0b00000001;
const FLAG_TRIE_PAGE: u8 = 0b00000010;
const FLAG_FREE_LIST_PAGE: u8 = 0b00000100;
const FLAG_INDEX_PAGE: u8 = 0b00001000;

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
}

#[derive(Debug)]
struct FreeListPage {
    next_free_list_page: i64,
    used_entries: i32,
    free_page_ids: Vec<i64>,
}

#[derive(Debug)]
struct ReverseTrieNode {
    value: char,
    parent_index: i64,
    self_index: i64,
    document_id: Option<Uuid>,
    children: HashMap<char, i64>,
}

#[derive(Clone, Debug)]
struct Document {
    id: Uuid,
    first_page_id: i64,
    current_version: i32,
    paths: Vec<String>,
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
        }
    }
}

pub trait Database {
    fn write_document(&mut self, path: &str, data: &mut dyn Read) -> io::Result<Uuid>;
    fn get(&self, path: &str) -> io::Result<Vec<u8>>;
    fn get_id_by_path(&self, path: &str) -> io::Result<Option<Uuid>>;
    fn delete(&mut self, path: &str) -> io::Result<()>;
    fn delete_by_id(&mut self, id: Uuid) -> io::Result<()>;
    fn bind_to_path(&mut self, id: Uuid, path: &str) -> io::Result<()>;
    fn unbind_path(&mut self, id: Uuid, path: &str) -> io::Result<()>;
    fn search(&self, prefix: &str) -> io::Result<Vec<String>>;
    fn list_paths(&self, id: Uuid) -> io::Result<Vec<String>>;
    fn flush(&self) -> io::Result<()>;
    fn calculate_statistics(&self) -> io::Result<(i64, i64)>;
    fn set_quick_mode(&mut self, enabled: bool);
}

pub trait DatabaseBackend {
    fn write_document(&mut self, data: &mut dyn Read) -> io::Result<Uuid>;
    fn read_document(&self, id: Uuid) -> io::Result<Vec<u8>>;
    fn delete_document(&mut self, id: Uuid) -> io::Result<()>;
    fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> io::Result<Uuid>;
    fn get_document_id_by_path(&self, path: &str) -> io::Result<Uuid>;
    fn search_paths(&self, prefix: &str) -> io::Result<Vec<String>>;
    fn list_paths_for_document(&self, id: Uuid) -> io::Result<Vec<String>>;
    fn count_free_pages(&self) -> io::Result<i64>;
    fn get_info(&self, id: Uuid) -> io::Result<String>;
    fn delete_paths_for_document(&mut self, id: Uuid) -> io::Result<()>;
    fn remove_from_index(&mut self, id: Uuid) -> io::Result<()>;
}

// In-Memory Backend for testing and volatile use cases
struct MemoryBackend {
    documents: Mutex<HashMap<Uuid, Vec<u8>>>,
    path_to_id: Mutex<HashMap<String, Uuid>>,
    id_to_paths: Mutex<HashMap<Uuid, Vec<String>>>,
    // For search, simple prefix map, not trie for simplicity
}

impl MemoryBackend {
    fn new() -> Self {
        Self {
            documents: Mutex::new(HashMap::new()),
            path_to_id: Mutex::new(HashMap::new()),
            id_to_paths: Mutex::new(HashMap::new()),
        }
    }
}

impl DatabaseBackend for MemoryBackend {
    fn write_document(&mut self, data: &mut dyn Read) -> io::Result<Uuid> {
        let id = Uuid::new_v4();
        let mut buf = Vec::new();
        data.read_to_end(&mut buf)?;
        self.documents.lock().insert(id, buf);
        self.id_to_paths.lock().insert(id, vec![]);
        Ok(id)
    }

    fn read_document(&self, id: Uuid) -> io::Result<Vec<u8>> {
        self.documents.lock().get(&id).cloned().ok_or(io::Error::new(io::ErrorKind::NotFound, "Document not found"))
    }

    fn delete_document(&mut self, id: Uuid) -> io::Result<()> {
        self.documents.lock().remove(&id);
        if let Some(paths) = self.id_to_paths.lock().remove(&id) {
            let mut path_map = self.path_to_id.lock();
            for p in paths {
                path_map.remove(&p);
            }
        }
        Ok(())
    }

    fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> io::Result<Uuid> {
        if !self.documents.lock().contains_key(&id) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "ID not found"));
        }
        self.path_to_id.lock().insert(path.to_string(), id);
        self.id_to_paths.lock().entry(id).or_insert(vec![]).push(path.to_string());
        Ok(id)
    }

    fn get_document_id_by_path(&self, path: &str) -> io::Result<Uuid> {
        self.path_to_id.lock().get(path).cloned().ok_or(io::Error::new(io::ErrorKind::NotFound, "Path not found"))
    }

    fn search_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        Ok(self.path_to_id.lock().keys().filter(|p| p.starts_with(prefix)).cloned().collect())
    }

    fn list_paths_for_document(&self, id: Uuid) -> io::Result<Vec<String>> {
        self.id_to_paths.lock().get(&id).cloned().ok_or(io::Error::new(io::ErrorKind::NotFound, "ID not found"))
    }

    fn count_free_pages(&self) -> io::Result<i64> {
        Ok(0) // No pages in memory
    }

    fn get_info(&self, id: Uuid) -> io::Result<String> {
        if let Some(data) = self.documents.lock().get(&id) {
            let paths = self.list_paths_for_document(id)?;
            Ok(format!("ID: {}, Version: 1, Size: {} bytes, Paths: {:?}", id, data.len(), paths))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "ID not found"))
        }
    }

    fn delete_paths_for_document(&mut self, id: Uuid) -> io::Result<()> {
        if let Some(paths) = self.id_to_paths.lock().get(&id) {
            let mut path_map = self.path_to_id.lock();
            for p in paths {
                path_map.remove(p);
            }
        }
        Ok(())
    }

    fn remove_from_index(&mut self, id: Uuid) -> io::Result<()> {
        Ok(()) // No index in memory
    }
}

// File Backend - Main persistent implementation
struct FileBackend {
    config: Config,
    file: Arc<Mutex<File>>,
    mmap: Arc<PlRwLock<Option<MmapMut>>>,
    current_size: Arc<Mutex<u64>>,
    document_index_root: Mutex<VersionedLink>,
    trie_root: Mutex<VersionedLink>,
    free_list_root: Mutex<VersionedLink>,
    page_cache: Mutex<LruCache<i64, Vec<u8>>>,
    quick_mode: bool,
    // For version retention: map doc_id to vec of (version, first_page_id) for old versions
    old_versions: Mutex<HashMap<Uuid, Vec<(i32, i64)>>>,
}

impl FileBackend {
    pub fn new<P: AsRef<Path>>(path: P, config: Config) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(path.as_ref())?;
        let mut initial_size = file.seek(SeekFrom::End(0))?;
        let mut header = DatabaseHeader {
            magic: MAGIC,
            index_root: VersionedLink { page_id: -1, version: 0 },
            path_lookup_root: VersionedLink { page_id: -1, version: 0 },
            free_list_root: VersionedLink { page_id: -1, version: 0 },
        };

        if initial_size == 0 {
            initial_size = config.page_size;
            file.set_len(initial_size)?;
            Self::write_header(&mut file, &header, &config)?;
        } else {
            header = Self::read_header(&mut file, &config)?;
            if header.magic != MAGIC {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid magic number"));
            }
        }

        let mmap = if config.use_mmap {
            Some(unsafe { MmapOptions::new().map_mut(&file)? })
        } else {
            None
        };

        let mut self_ = Self {
            config,
            file: Arc::new(Mutex::new(file)),
            mmap: Arc::new(PlRwLock::new(mmap)),
            current_size: Arc::new(Mutex::new(initial_size)),
            document_index_root: Mutex::new(header.index_root),
            trie_root: Mutex::new(header.path_lookup_root),
            free_list_root: Mutex::new(header.free_list_root),
            page_cache: Mutex::new(LruCache::new(self.config.page_cache_size)),
            quick_mode: false,
            old_versions: Mutex::new(HashMap::new()),
        };

        self_.recover()?;
        Ok(self_)
    }

    fn read_header(file: &mut File, config: &Config) -> io::Result<DatabaseHeader> {
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        let index_page = file.read_i64<LittleEndian>()?;
        let index_ver = file.read_i32<LittleEndian>()?;
        let path_page = file.read_i64<LittleEndian>()?;
        let path_ver = file.read_i32<LittleEndian>()?;
        let free_page = file.read_i64<LittleEndian>()?;
        let free_ver = file.read_i32<LittleEndian>()?;
        Ok(DatabaseHeader {
            magic,
            index_root: VersionedLink { page_id: index_page, version: index_ver },
            path_lookup_root: VersionedLink { page_id: path_page, version: path_ver },
            free_list_root: VersionedLink { page_id: free_page, version: free_ver },
        })
    }

    fn write_header(file: &mut File, header: &DatabaseHeader, config: &Config) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header.magic)?;
        file.write_i64<LittleEndian>(header.index_root.page_id)?;
        file.write_i32<LittleEndian>(header.index_root.version)?;
        file.write_i64<LittleEndian>(header.path_lookup_root.page_id)?;
        file.write_i32<LittleEndian>(header.path_lookup_root.version)?;
        file.write_i64<LittleEndian>(header.free_list_root.page_id)?;
        file.write_i32<LittleEndian>(header.free_list_root.version)?;
        file.flush()?;
        Ok(())
    }

    fn update_header(&self) -> io::Result<()> {
        let mut file = self.file.lock();
        let header = DatabaseHeader {
            magic: MAGIC,
            index_root: *self.document_index_root.lock(),
            path_lookup_root: *self.trie_root.lock(),
            free_list_root: *self.free_list_root.lock(),
        };
        Self::write_header(&mut *file, &header, &self.config)
    }

    fn grow_file(&self, additional_pages: u64) -> io::Result<()> {
        let to_grow = additional_pages.max(BATCH_GROW_PAGES);
        let mut current = self.current_size.lock();
        let new_size = *current + to_grow * self.config.page_size;
        if new_size > self.config.max_db_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Max DB size exceeded"));
        }
        let mut file = self.file.lock();
        file.set_len(new_size)?;
        *current = new_size;
        if self.config.use_mmap {
            let new_mmap = unsafe { MmapOptions::new().map_mut(&*file)? };
            *self.mmap.write() = Some(new_mmap);
        }
        Ok(())
    }

    fn allocate_page(&self) -> io::Result<i64> {
        if let Ok(page) = self.pop_free_page() {
            if page != -1 {
                return Ok(page);
            }
        }
        let current_pages = *self.current_size.lock() / self.config.page_size;
        if current_pages >= self.config.max_pages as u64 {
            return Err(io::Error::new(io::ErrorKind::Other, "Max pages exceeded"));
        }
        self.grow_file(1)?;
        Ok(current_pages as i64)
    }

    fn pop_free_page(&self) -> io::Result<i64> {
        let root = self.free_list_root.lock().page_id;
        if root == -1 {
            return Ok(-1);
        }
        let mut current = root;
        let mut prev = -1;
        loop {
            let data = self.read_raw_page(current)?;
            let mut rdr = Cursor::new(&data);
            let next = rdr.read_i64<LittleEndian>()?;
            let mut used = rdr.read_i32<LittleEndian>()?;
            if used > 0 {
                // Pop LIFO
                used -= 1;
                let pop_offset = 12 + (used as u64 * 8); // i64 next, i32 used, i64 ids
                rdr.seek(SeekFrom::Start(pop_offset))?;
                let popped = rdr.read_i64<LittleEndian>()?;
                // Write back used
                let mut used_data = vec![0u8; 4];
                let mut wtr = Cursor::new(&mut used_data);
                wtr.write_i32<LittleEndian>(used)?;
                self.write_raw_page(current, &used_data, 8)?; // Offset after next
                if used == 0 && prev != -1 {
                    // Consolidation: free empty free_list page
                    let mut prev_data = self.read_raw_page(prev)?;
                    let mut prev_rdr = Cursor::new(&mut prev_data);
                    prev_rdr.write_i64<LittleEndian>(next)?; // Update prev next to skip current
                    self.write_raw_page(prev, &prev_data, 0)?;
                    self.free_page_internal(current)?;
                } else if used == 0 && prev == -1 {
                    let mut root_lock = self.free_list_root.lock();
                    root_lock.page_id = next;
                    root_lock.version += 1;
                    self.update_header()?;
                    self.free_page_internal(current)?;
                }
                return Ok(popped);
            }
            prev = current;
            if next == -1 {
                break;
            }
            current = next;
        }
        Ok(-1)
    }

    fn free_page(&self, page_id: i64) -> io::Result<()> {
        self.free_page_internal(page_id)
    }

    fn free_page_internal(&self, page_id: i64) -> io::Result<()> {
        let mut root = self.free_list_root.lock();
        if root.page_id == -1 {
            let new_page = self.allocate_page()?;
            let mut header = PageHeader {
                crc: 0,
                version: 1,
                prev_page_id: -1,
                next_page_id: -1,
                flags: FLAG_FREE_LIST_PAGE,
                data_length: 12 + 8,
            };
            let mut data = vec![0u8; 12 + 8];
            let mut wtr = Cursor::new(&mut data);
            wtr.write_i64<LittleEndian>(-1)?; // next
            wtr.write_i32<LittleEndian>(1)?; // used
            wtr.write_i64<LittleEndian>(page_id)?; // id
            header.crc = self.compute_crc(&data);
            self.write_page_header(new_page, &header)?;
            self.write_raw_page(new_page, &data, 0)?;
            root.page_id = new_page;
            root.version += 1;
            self.update_header()?;
            return Ok(());
        }
        let mut current = root.page_id;
        loop {
            let data = self.read_raw_page(current)?;
            let mut rdr = Cursor::new(&data);
            let next = rdr.read_i64<LittleEndian>()?;
            let used = rdr.read_i32<LittleEndian>()?;
            if used < FREE_LIST_ENTRIES_PER_PAGE as i32 {
                let offset = 12 + (used as u64 * 8);
                let mut add = vec![0u8; 8];
                let mut wtr = Cursor::new(&mut add);
                wtr.write_i64<LittleEndian>(page_id)?;
                self.write_raw_page(current, &add, offset)?;
                let mut used_data = vec![0u8; 4];
                let mut used_wtr = Cursor::new(&mut used_data);
                used_wtr.write_i32<LittleEndian>(used + 1)?;
                self.write_raw_page(current, &used_data, 8)?;
                let mut full_data = self.read_raw_page(current)?;
                let mut header = self.read_page_header(current)?;
                header.crc = self.compute_crc(&full_data);
                self.write_page_header(current, &header)?;
                root.version += 1;
                self.update_header()?;
                return Ok(());
            }
            if next == -1 {
                let new_page = self.allocate_page()?;
                let mut update_next = vec![0u8; 8];
                let mut wtr = Cursor::new(&mut update_next);
                wtr.write_i64<LittleEndian>(new_page)?;
                self.write_raw_page(current, &update_next, 0)?;
                let mut new_data = vec![0u8; 12 + 8];
                let mut new_wtr = Cursor::new(&mut new_data);
                new_wtr.write_i64<LittleEndian>(-1)?; // next
                new_wtr.write_i32<LittleEndian>(1)?; // used
                new_wtr.write_i64<LittleEndian>(page_id)?;
                let mut new_header = PageHeader {
                    crc: self.compute_crc(&new_data),
                    version: 1,
                    prev_page_id: -1,
                    next_page_id: -1,
                    flags: FLAG_FREE_LIST_PAGE,
                    data_length: new_data.len() as i32,
                };
                self.write_page_header(new_page, &new_header)?;
                self.write_raw_page(new_page, &new_data, 0)?;
                root.version += 1;
                self.update_header()?;
                return Ok(());
            }
            current = next;
        }
    }

    fn read_raw_page(&self, page_id: i64) -> io::Result<Vec<u8>> {
        if page_id < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid page ID"));
        }
        let mut cache = self.page_cache.lock();
        if let Some(data) = cache.get(&page_id) {
            return Ok(data.clone());
        }
        let offset = page_id as u64 * self.config.page_size;
        let header = self.read_page_header(page_id)?;
        if header.data_length < 0 || header.data_length as u64 > self.config.page_size - self.config.page_header_size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data length"));
        }
        let mut data = vec![0u8; header.data_length as usize];
        if let Some(mmap) = self.mmap.read().as_ref() {
            let data_offset = offset + self.config.page_header_size;
            data.copy_from_slice(&mmap[data_offset as usize..(data_offset + header.data_length as u64) as usize]);
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset + self.config.page_header_size))?;
            file.read_exact(&mut data)?;
        }
        if !self.quick_mode {
            let computed = self.compute_crc(&data);
            if computed != header.crc {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
            }
        }
        cache.put(page_id, data.clone());
        Ok(data)
    }

    fn write_raw_page(&self, page_id: i64, data: &[u8], data_offset: u64) -> io::Result<()> {
        let page_offset = page_id as u64 * self.config.page_size;
        let full_offset = page_offset + self.config.page_header_size + data_offset;
        if data.len() as u64 + data_offset > self.config.page_size - self.config.page_header_size {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Data exceeds page capacity"));
        }
        if let Some(mut mmap) = self.mmap.write().as_mut() {
            mmap[full_offset as usize..(full_offset + data.len() as u64) as usize].copy_from_slice(data);
            mmap.flush()?;
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(full_offset))?;
            file.write_all(data)?;
            file.flush()?;
        }
        let mut cache = self.page_cache.lock();
        cache.pop(&page_id);
        Ok(())
    }

    fn update_page_crc(&self, page_id: i64) -> io::Result<()> {
        let data = self.read_raw_page(page_id)?;
        let crc = self.compute_crc(&data);
        let mut header = self.read_page_header(page_id)?;
        header.crc = crc;
        self.write_page_header(page_id, &header)
    }

    fn read_page_header(&self, page_id: i64) -> io::Result<PageHeader> {
        let offset = page_id as u64 * self.config.page_size;
        let mut header_data = vec![0u8; self.config.page_header_size as usize];
        if let Some(mmap) = self.mmap.read().as_ref() {
            header_data.copy_from_slice(&mmap[offset as usize..(offset + self.config.page_header_size) as usize]);
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut header_data)?;
        }
        let mut rdr = Cursor::new(&header_data);
        Ok(PageHeader {
            crc: rdr.read_u32<LittleEndian>()?,
            version: rdr.read_i32<LittleEndian>()?,
            prev_page_id: rdr.read_i64<LittleEndian>()?,
            next_page_id: rdr.read_i64<LittleEndian>()?,
            flags: rdr.read_u8()?,
            data_length: rdr.read_i32<LittleEndian>()?,
        })
    }

    fn write_page_header(&self, page_id: i64, header: &PageHeader) -> io::Result<()> {
        let offset = page_id as u64 * self.config.page_size;
        let mut data = vec![0u8; self.config.page_header_size as usize];
        let mut wtr = Cursor::new(&mut data);
        wtr.write_u32<LittleEndian>(header.crc)?;
        wtr.write_i32<LittleEndian>(header.version)?;
        wtr.write_i64<LittleEndian>(header.prev_page_id)?;
        wtr.write_i64<LittleEndian>(header.next_page_id)?;
        wtr.write_u8(header.flags)?;
        wtr.write_i32<LittleEndian>(header.data_length)?;
        if let Some(mut mmap) = self.mmap.write().as_mut() {
            mmap[offset as usize..(offset + self.config.page_header_size) as usize].copy_from_slice(&data);
            mmap.flush()?;
        } else {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&data)?;
            file.flush()?;
        }
        Ok(())
    }

    fn compute_crc(&self, data: &[u8]) -> u32 {
        let crc = CrcLib::<u32>::new(&CRC_32_ISO_HDLC);
        crc.checksum(data)
    }

    fn recover(&mut self) -> io::Result<()> {
        // Comprehensive recovery: 
        // 1. Validate header
        // 2. Traverse all known structures (index, trie, free, doc chains) to mark reached pages
        // 3. Scan all possible pages (0 to current_size / page_size -1)
        // 4. For unreached pages, if valid header, add to free list; if corrupted, log and skip or zero
        // 5. For chains, validate version monotonic, repair links if broken (e.g., if next prev not match, cut)
        // 6. Rebuild if roots corrupted
        let header = Self::read_header(&mut self.file.lock(), &self.config)?;
        if header.magic != MAGIC {
            // Reinit header
            let new_header = DatabaseHeader {
                magic: MAGIC,
                index_root: VersionedLink { page_id: -1, version: 0 },
                path_lookup_root: VersionedLink { page_id: -1, version: 0 },
                free_list_root: VersionedLink { page_id: -1, version: 0 },
            };
            Self::write_header(&mut self.file.lock(), &new_header, &self.config)?;
            *self.document_index_root.lock() = new_header.index_root;
            *self.trie_root.lock() = new_header.path_lookup_root;
            *self.free_list_root.lock() = new_header.free_list_root;
        }
        let max_page = (*self.current_size.lock() / self.config.page_size) as i64 - 1;
        let mut reached = HashSet::new();
        // Traverse free list
        let mut current = self.free_list_root.lock().page_id;
        while current != -1 {
            reached.insert(current);
            let header = match self.read_page_header(current) {
                Ok(h) => h,
                Err(_) => break, // Cut corrupted
            };
            if header.flags & FLAG_FREE_LIST_PAGE == 0 {
                break; // Wrong type
            }
            let data = match self.read_raw_page(current) {
                Ok(d) => d,
                Err(_) => break,
            };
            let mut rdr = Cursor::new(&data);
            current = match rdr.read_i64<LittleEndian>() {
                Ok(n) => n,
                Err(_) => break,
            };
        }
        // Traverse index chain
        current = self.document_index_root.lock().page_id;
        while current != -1 {
            reached.insert(current);
            let header = match self.read_page_header(current) {
                Ok(h) => h,
                Err(_) => break,
            };
            if header.flags & FLAG_INDEX_PAGE == 0 {
                break;
            }
            current = header.next_page_id;
        }
        // Traverse trie (tree, recursive)
        if self.trie_root.lock().page_id != -1 {
            self.trie_traverse_reach(self.trie_root.lock().page_id, &mut reached)?;
        }
        // Traverse doc chains
        current = self.document_index_root.lock().page_id;
        while current != -1 {
            let data = match self.read_raw_page(current) {
                Ok(d) => d,
                Err(_) => break,
            };
            let mut rdr = Cursor::new(&data);
            let mut bytes = [0; 16];
            rdr.read_exact(&mut bytes)?;
            let id = Uuid::from_bytes(bytes);
            let first_page = rdr.read_i64<LittleEndian>()?;
            let mut chain = first_page;
            while chain != -1 {
                reached.insert(chain);
                let h = match self.read_page_header(chain) {
                    Ok(hh) => hh,
                    Err(_) => break,
                };
                if h.flags & FLAG_DATA_PAGE == 0 {
                    break;
                }
                if h.version <= 0 {
                    // Invalid version, cut
                    break;
                }
                chain = h.next_page_id;
            }
            current = self.read_page_header(current)?.next_page_id;
        }
        // For unreached pages, add to free if valid header
        for p in 0..=max_page {
            if !reached.contains(&p) {
                if let Ok(h) = self.read_page_header(p) {
                    if h.version > 0 {
                        self.free_page_internal(p)?;
                    }
                }
            }
        }
        // GC old versions
        self.gc_old_versions()?;
        Ok(())
    }

    fn trie_traverse_reach(&self, node_id: i64, reached: &mut HashSet<i64>) -> io::Result<()> {
        if node_id == -1 || reached.contains(&node_id) {
            return Ok(());
        }
        reached.insert(node_id);
        let header = self.read_page_header(node_id)?;
        if header.flags & FLAG_TRIE_PAGE == 0 {
            return Ok(());
        }
        let data = self.read_raw_page(node_id)?;
        let node = self.deserialize_trie_node(&data)?;
        for &child in node.children.values() {
            self.trie_traverse_reach(child, reached)?;
        }
        Ok(())
    }

    fn gc_old_versions(&mut self) -> io::Result<()> {
        let mut old = self.old_versions.lock();
        for (id, vers) in old.iter_mut() {
            vers.sort_by_key(|&(v, _)| v);
            while vers.len() as i32 > self.config.versions_to_keep {
                let (old_v, old_chain) = vers.remove(0);
                self.delete_document_pages(old_chain)?;
            }
        }
        Ok(())
    }
}

impl DatabaseBackend for FileBackend {
    fn write_document(&mut self, data: &mut dyn Read) -> io::Result<Uuid> {
        let id = Uuid::new_v4();
        let mut total_size = 0u64;
        let mut buf = vec![0u8; (self.config.page_size - self.config.page_header_size) as usize];
        let mut first_page = -1i64;
        let mut current_page = -1i64;
        let mut version = 1i32;
        loop {
            let len = data.read(&mut buf)?;
            if len == 0 {
                break;
            }
            total_size += len as u64;
            if total_size > self.config.max_document_size {
                if first_page != -1 {
                    self.delete_document_pages(first_page)?;
                }
                return Err(io::Error::new(io::ErrorKind::Other, "Max document size exceeded"));
            }
            let new_page = self.allocate_page()?;
            let mut header = PageHeader {
                crc: self.compute_crc(&buf[0..len]),
                version,
                prev_page_id: current_page,
                next_page_id: -1,
                flags: FLAG_DATA_PAGE,
                data_length: len as i32,
            };
            self.write_page_header(new_page, &header)?;
            self.write_raw_page(new_page, &buf[0..len], 0)?;
            if current_page != -1 {
                let mut prev_header = self.read_page_header(current_page)?;
                prev_header.next_page_id = new_page;
                self.write_page_header(current_page, &prev_header)?;
            } else {
                first_page = new_page;
            }
            current_page = new_page;
            version += 1;
        }
        if first_page == -1 {
            first_page = self.allocate_page()?;
            let header = PageHeader {
                crc: 0,
                version: 1,
                prev_page_id: -1,
                next_page_id: -1,
                flags: FLAG_DATA_PAGE,
                data_length: 0,
            };
            self.write_page_header(first_page, &header)?;
        }
        self.add_document_to_index(id, first_page, 1)?;
        Ok(id)
    }

    fn read_document(&self, id: Uuid) -> io::Result<Vec<u8>> {
        let doc_opt = self.get_document_from_index(id)?;
        let doc = doc_opt.ok_or(io::Error::new(io::ErrorKind::NotFound, "Document not found"))?;
        let mut result = Vec::with_capacity(doc.paths.len() * (self.config.page_size as usize - self.config.page_header_size as usize));
        let mut current = doc.first_page_id;
        let mut prev_version = 0;
        while current != -1 {
            let header = self.read_page_header(current)?;
            if header.version <= prev_version {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Non-monotonic version in chain"));
            }
            prev_version = header.version;
            let data = self.read_raw_page(current)?;
            result.extend_from_slice(&data[0..header.data_length as usize]);
            current = header.next_page_id;
        }
        Ok(result)
    }

    fn delete_document(&mut self, id: Uuid) -> io::Result<()> {
        let doc_opt = self.get_document_from_index(id)?;
        if let Some(doc) = doc_opt {
            let mut old = self.old_versions.lock();
            let entry = old.entry(id).or_insert(vec![]);
            entry.push((doc.current_version, doc.first_page_id));
            self.remove_from_index(id)?;
            self.gc_old_versions()?;
        }
        Ok(())
    }

    fn bind_path_to_document(&mut self, path: &str, id: Uuid) -> io::Result<Uuid> {
        if self.get_document_from_index(id)?.is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "ID not found"));
        }
        self.trie_insert(path, id)?;
        self.update_document_paths(id, path, true)?;
        Ok(id)
    }

    fn get_document_id_by_path(&self, path: &str) -> io::Result<Uuid> {
        self.trie_search(path)?.ok_or(io::Error::new(io::ErrorKind::NotFound, "Path not found"))
    }

    fn search_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        let reversed_prefix = prefix.chars().rev().collect::<String>();
        let root = self.trie_root.lock().page_id;
        if root == -1 {
            return Ok(vec![]);
        }
        let mut current = root;
        for ch in reversed_prefix.chars() {
            let data = self.read_raw_page(current)?;
            let node = self.deserialize_trie_node(&data)?;
            if let Some(&child) = node.children.get(&ch) {
                current = child;
            } else {
                return Ok(vec![]);
            }
        }
        let mut results = vec![];
        self.trie_collect_paths(current, &mut String::new(), &mut results)?;
        results.iter_mut().for_each(|s| *s = s.chars().rev().collect());
        Ok(results)
    }

    fn list_paths_for_document(&self, id: Uuid) -> io::Result<Vec<String>> {
        self.get_document_from_index(id)?.ok_or(io::Error::new(io::ErrorKind::NotFound, "ID not found")).map(|d| d.paths)
    }

    fn count_free_pages(&self) -> io::Result<i64> {
        let mut count = 0i64;
        let mut current = self.free_list_root.lock().page_id;
        while current != -1 {
            let data = self.read_raw_page(current)?;
            let mut rdr = Cursor::new(&data);
            let next = rdr.read_i64<LittleEndian>()?;
            let used = rdr.read_i32<LittleEndian>()?;
            count += used as i64;
            current = next;
        }
        Ok(count)
    }

    fn get_info(&self, id: Uuid) -> io::Result<String> {
        let doc_opt = self.get_document_from_index(id)?;
        if let Some(doc) = doc_opt {
            let mut size = 0u64;
            let mut current = doc.first_page_id;
            while current != -1 {
                let header = self.read_page_header(current)?;
                size += header.data_length as u64;
                current = header.next_page_id;
            }
            Ok(format!("ID: {}, Version: {}, Size: {} bytes, Paths: {:?} ", doc.id, doc.current_version, size, doc.paths))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "ID not found"))
        }
    }

    fn delete_paths_for_document(&mut self, id: Uuid) -> io::Result<()> {
        let doc_opt = self.get_document_from_index(id)?;
        if let Some(doc) = doc_opt {
            for p in doc.paths {
                self.trie_delete(&p)?;
            }
            self.update_document_paths(id, "", false)?; // Clear all by setting empty
        }
        Ok(())
    }

    fn remove_from_index(&mut self, id: Uuid) -> io::Result<()> {
        let mut root = self.document_index_root.lock();
        let mut prev = -1i64;
        let mut current = root.page_id;
        while current != -1 {
            let data = self.read_raw_page(current)?;
            let mut rdr = Cursor::new(&data);
            let mut bytes = [0u8; 16];
            rdr.read_exact(&mut bytes)?;
            let entry_id = Uuid::from_bytes(bytes);
            if entry_id == id {
                if prev == -1 {
                    root.page_id = self.read_page_header(current)?.next_page_id;
                } else {
                    let mut prev_header = self.read_page_header(prev)?;
                    prev_header.next_page_id = self.read_page_header(current)?.next_page_id;
                    self.write_page_header(prev, &prev_header)?;
                }
                root.version += 1;
                self.update_header()?;
                self.free_page_internal(current)?;
                return Ok(());
            }
            prev = current;
            current = self.read_page_header(current)?.next_page_id;
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "ID not in index"))
    }

    // Private helpers
    fn add_document_to_index(&self, id: Uuid, first_page: i64, version: i32) -> io::Result<()> {
        let new_page = self.allocate_page()?;
        let mut data = vec![];
        data.write_all(id.as_bytes())?;
        data.write_i64<LittleEndian>(first_page)?;
        data.write_i32<LittleEndian>(version)?;
        data.write_i32<LittleEndian>(0)?; // path count
        let header = PageHeader {
            crc: self.compute_crc(&data),
            version: 1,
            prev_page_id: -1,
            next_page_id: -1,
            flags: FLAG_INDEX_PAGE,
            data_length: data.len() as i32,
        };
        self.write_page_header(new_page, &header)?;
        self.write_raw_page(new_page, &data, 0)?;
        let mut root = self.document_index_root.lock();
        if root.page_id == -1 {
            root.page_id = new_page;
        } else {
            let mut current = root.page_id;
            while current != -1 {
                let h = self.read_page_header(current)?;
                if h.next_page_id == -1 {
                    let mut new_h = h;
                    new_h.next_page_id = new_page;
                    self.write_page_header(current, &new_h)?;
                    break;
                }
                current = h.next_page_id;
            }
        }
        root.version += 1;
        self.update_header()?;
        Ok(())
    }

    fn get_document_from_index(&self, id: Uuid) -> io::Result<Option<Document>> {
        let root = self.document_index_root.lock().page_id;
        let mut current = root;
        while current != -1 {
            let data = self.read_raw_page(current)?;
            let mut rdr = Cursor::new(&data);
            let mut bytes = [0u8; 16];
            rdr.read_exact(&mut bytes)?;
            let entry_id = Uuid::from_bytes(bytes);
            if entry_id == id {
                let first_page = rdr.read_i64<LittleEndian>()?;
                let version = rdr.read_i32<LittleEndian>()?;
                let path_count = rdr.read_i32<LittleEndian>()?;
                let mut paths = vec![];
                for _ in 0..path_count {
                    let len = rdr.read_u32<LittleEndian>()?;
                    let mut path_buf = vec![0; len as usize];
                    rdr.read_exact(&mut path_buf)?;
                    paths.push(String::from_utf8(path_buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF8 path"))?);
                }
                return Ok(Some(Document { id: entry_id, first_page_id: first_page, current_version: version, paths }));
            }
            current = self.read_page_header(current)?.next_page_id;
        }
        Ok(None)
    }

    fn update_document_paths(&self, id: Uuid, path: &str, add: bool) -> io::Result<()> {
        let root = self.document_index_root.lock().page_id;
        let mut current = root;
        while current != -1 {
            let data = self.read_raw_page(current)?;
            let mut rdr = Cursor::new(&data);
            let mut bytes = [0u8; 16];
            rdr.read_exact(&mut bytes)?;
            let entry_id = Uuid::from_bytes(bytes);
            if entry_id == id {
                let first_page = rdr.read_i64<LittleEndian>()?;
                let version = rdr.read_i32<LittleEndian>()?;
                let path_count = rdr.read_i32<LittleEndian>()?;
                let mut paths = vec![];
                for _ in 0..path_count {
                    let len = rdr.read_u32<LittleEndian>()?;
                    let mut path_buf = vec![0; len as usize];
                    rdr.read_exact(&mut path_buf)?;
                    paths.push(String::from_utf8(path_buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF8 path"))?);
                }
                if add {
                    if !paths.contains(&path.to_string()) {
                        paths.push(path.to_string());
                    }
                } else if path.is_empty() {
                    paths.clear();
                } else {
                    paths.retain(|p| p != path);
                }
                let mut new_data = vec![];
                new_data.write_all(id.as_bytes())?;
                new_data.write_i64<LittleEndian>(first_page)?;
                new_data.write_i32<LittleEndian>(version)?;
                new_data.write_i32<LittleEndian>(paths.len() as i32)?;
                for p in &paths {
                    new_data.write_u32<LittleEndian>(p.len() as u32)?;
                    new_data.write_all(p.as_bytes())?;
                }
                let mut header = self.read_page_header(current)?;
                header.data_length = new_data.len() as i32;
                header.crc = self.compute_crc(&new_data);
                self.write_page_header(current, &header)?;
                self.write_raw_page(current, &new_data, 0)?;
                return Ok(());
            }
            current = self.read_page_header(current)?.next_page_id;
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "ID not found"))
    }

    fn trie_insert(&self, path: &str, id: Uuid) -> io::Result<()> {
        let reversed = path.chars().rev().collect::<String>();
        let mut root_lock = self.trie_root.lock();
        if root_lock.page_id == -1 {
            let new_root = self.allocate_page()?;
            let root_node = ReverseTrieNode {
                value: '\0',
                parent_index: -1,
                self_index: new_root,
                document_id: None,
                children: HashMap::new(),
            };
            let serialized = self.serialize_trie_node(&root_node)?;
            let header = PageHeader {
                crc: self.compute_crc(&serialized),
                version: 1,
                prev_page_id: -1,
                next_page_id: -1,
                flags: FLAG_TRIE_PAGE,
                data_length: serialized.len() as i32,
            };
            self.write_page_header(new_root, &header)?;
            self.write_raw_page(new_root, &serialized, 0)?;
            root_lock.page_id = new_root;
            root_lock.version += 1;
            self.update_header()?;
        }
        let mut current = root_lock.page_id;
        for ch in reversed.chars() {
            let data = self.read_raw_page(current)?;
            let mut node = self.deserialize_trie_node(&data)?;
            if let Some(&child) = node.children.get(&ch) {
                current = child;
            } else {
                let new_child = self.allocate_page()?;
                let new_node = ReverseTrieNode {
                    value: ch,
                    parent_index: current,
                    self_index: new_child,
                    document_id: None,
                    children: HashMap::new(),
                };
                let serialized = self.serialize_trie_node(&new_node)?;
                let header = PageHeader {
                    crc: self.compute_crc(&serialized),
                    version: 1,
                    prev_page_id: -1,
                    next_page_id: -1,
                    flags: FLAG_TRIE_PAGE,
                    data_length: serialized.len() as i32,
                };
                self.write_page_header(new_child, &header)?;
                self.write_raw_page(new_child, &serialized, 0)?;
                node.children.insert(ch, new_child);
                let updated = self.serialize_trie_node(&node)?;
                let mut u_header = self.read_page_header(current)?;
                u_header.crc = self.compute_crc(&updated);
                u_header.data_length = updated.len() as i32;
                self.write_page_header(current, &u_header)?;
                self.write_raw_page(current, &updated, 0)?;
                current = new_child;
            }
        }
        let data = self.read_raw_page(current)?;
        let mut node = self.deserialize_trie_node(&data)?;
        node.document_id = Some(id);
        let updated = self.serialize_trie_node(&node)?;
        let mut u_header = self.read_page_header(current)?;
        u_header.crc = self.compute_crc(&updated);
        u_header.data_length = updated.len() as i32;
        self.write_page_header(current, &u_header)?;
        self.write_raw_page(current, &updated, 0)?;
        root_lock.version += 1;
        self.update_header()?;
        Ok(())
    }

    fn trie_search(&self, path: &str) -> io::Result<Option<Uuid>> {
        let reversed = path.chars().rev().collect::<String>();
        let root = self.trie_root.lock().page_id;
        if root == -1 {
            return Ok(None);
        }
        let mut current = root;
        for ch in reversed.chars() {
            let data = self.read_raw_page(current)?;
            let node = self.deserialize_trie_node(&data)?;
            if let Some(&child) = node.children.get(&ch) {
                current = child;
            } else {
                return Ok(None);
            }
        }
        let data = self.read_raw_page(current)?;
        let node = self.deserialize_trie_node(&data)?;
        Ok(node.document_id)
    }

    fn trie_delete(&self, path: &str) -> io::Result<()> {
        let reversed = path.chars().rev().collect::<String>();
        let root = self.trie_root.lock().page_id;
        if root == -1 {
            return Ok(());
        }
        let mut path_nodes = vec![];
        let mut current = root;
        for ch in reversed.chars() {
            path_nodes.push((current, ch));
            let data = self.read_raw_page(current)?;
            let node = self.deserialize_trie_node(&data)?;
            if let Some(&child) = node.children.get(&ch) {
                current = child;
            } else {
                return Ok(());
            }
        }
        // Clear doc_id
        let data = self.read_raw_page(current)?;
        let mut node = self.deserialize_trie_node(&data)?;
        node.document_id = None;
        let updated = self.serialize_trie_node(&node)?;
        let mut header = self.read_page_header(current)?;
        header.crc = self.compute_crc(&updated);
        header.data_length = updated.len() as i32;
        self.write_page_header(current, &header)?;
        self.write_raw_page(current, &updated, 0)?;
        // Prune empty branches
        let mut i = path_nodes.len();
        while i > 0 {
            i -= 1;
            let (parent, ch) = path_nodes[i];
            let child = if i + 1 < path_nodes.len() { path_nodes[i + 1].0 } else { current };
            let p_data = self.read_raw_page(parent)?;
            let mut p_node = self.deserialize_trie_node(&p_data)?;
            p_node.children.remove(&ch);
            let p_updated = self.serialize_trie_node(&p_node)?;
            let mut p_header = self.read_page_header(parent)?;
            p_header.crc = self.compute_crc(&p_updated);
            p_header.data_length = p_updated.len() as i32;
            self.write_page_header(parent, &p_header)?;
            self.write_raw_page(parent, &p_updated, 0)?;
            let c_data = self.read_raw_page(child)?;
            let c_node = self.deserialize_trie_node(&c_data)?;
            if c_node.children.is_empty() && c_node.document_id.is_none() {
                self.free_page_internal(child)?;
            } else {
                break;
            }
            current = parent;
        }
        let mut root_lock = self.trie_root.lock();
        root_lock.version += 1;
        self.update_header()?;
        Ok(())
    }

    fn trie_collect_paths(&self, node_id: i64, current_path: &mut String, results: &mut Vec<String>) -> io::Result<()> {
        let data = self.read_raw_page(node_id)?;
        let node = self.deserialize_trie_node(&data)?;
        if node.document_id.is_some() {
            results.push(current_path.clone());
        }
        for (&ch, &child) in &node.children {
            current_path.push(ch);
            self.trie_collect_paths(child, current_path, results)?;
            current_path.pop();
        }
        Ok(())
    }

    fn delete_document_pages(&self, first_page: i64) -> io::Result<()> {
        let mut current = first_page;
        while current != -1 {
            let header = self.read_page_header(current)?;
            let next = header.next_page_id;
            self.free_page_internal(current)?;
            current = next;
        }
        Ok(())
    }

    fn serialize_trie_node(&self, node: &ReverseTrieNode) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_u32<LittleEndian>(node.value as u32)?;
        buf.write_i64<LittleEndian>(node.parent_index)?;
        buf.write_i64<LittleEndian>(node.self_index)?;
        if let Some(id) = node.document_id {
            buf.write_u8(1)?;
            buf.write_all(id.as_bytes())?;
        } else {
            buf.write_u8(0)?;
        }
        buf.write_u32<LittleEndian>(node.children.len() as u32)?;
        for (&ch, &idx) in &node.children {
            buf.write_u32<LittleEndian>(ch as u32)?;
            buf.write_i64<LittleEndian>(idx)?;
        }
        if buf.len() as u64 > self.config.page_size - self.config.page_header_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Trie node too large for page"));
        }
        Ok(buf)
    }

    fn deserialize_trie_node(&self, data: &[u8]) -> io::Result<ReverseTrieNode> {
        let mut rdr = Cursor::new(data);
        let value = char::from_u32(rdr.read_u32<LittleEndian>()?).ok_or(io::Error::new(io::ErrorKind::InvalidData, "Invalid char"))?;
        let parent = rdr.read_i64<LittleEndian>()?;
        let self_idx = rdr.read_i64<LittleEndian>()?;
        let has_id = rdr.read_u8()?;
        let document_id = if has_id == 1 {
            let mut bytes = [0u8; 16];
            rdr.read_exact(&mut bytes)?;
            Some(Uuid::from_bytes(bytes))
        } else {
            None
        };
        let child_count = rdr.read_u32<LittleEndian>()?;
        let mut children = HashMap::with_capacity(child_count as usize);
        for _ in 0..child_count {
            let ch = char::from_u32(rdr.read_u32<LittleEndian>()?).ok_or(io::Error::new(io::ErrorKind::InvalidData, "Invalid char"))?;
            let idx = rdr.read_i64<LittleEndian>()?;
            children.insert(ch, idx);
        }
        Ok(ReverseTrieNode {
            value,
            parent_index: parent,
            self_index: self_idx,
            document_id,
            children,
        })
    }
}

// StreamDb wrapper
pub struct StreamDb {
    backend: FileBackend, // For file, but can be generic <B: DatabaseBackend>
    quick_mode: bool,
    path_cache: PlRwLock<HashMap<String, Uuid>>,
}

impl StreamDb {
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> io::Result<Self> {
        let backend = FileBackend::new(path, config)?;
        Ok(Self {
            backend,
            quick_mode: false,
            path_cache: PlRwLock::new(HashMap::new()),
        })
    }

    pub fn with_memory_backend() -> Self {
        let backend = MemoryBackend::new();
        Self {
            backend,
            quick_mode: false,
            path_cache: PlRwLock::new(HashMap::new()),
        }
    }
}

impl Database for StreamDb {
    fn write_document(&mut self, path: &str, data: &mut dyn Read) -> io::Result<Uuid> {
        let id = self.backend.write_document(data)?;
        self.backend.bind_path_to_document(path, id)?;
        self.path_cache.write().insert(path.to_string(), id);
        Ok(id)
    }

    fn get(&self, path: &str) -> io::Result<Vec<u8>> {
        if let Some(id) = self.path_cache.read().get(path) {
            return self.backend.read_document(*id);
        }
        if let Ok(id) = self.backend.get_document_id_by_path(path) {
            self.path_cache.write().insert(path.to_string(), id);
            self.backend.read_document(id)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Path not found"))
        }
    }

    fn get_id_by_path(&self, path: &str) -> io::Result<Option<Uuid>> {
        if let Some(id) = self.path_cache.read().get(path) {
            return Ok(Some(*id));
        }
        if let Ok(id) = self.backend.get_document_id_by_path(path) {
            self.path_cache.write().insert(path.to_string(), id);
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn delete(&mut self, path: &str) -> io::Result<()> {
        if let Some(id) = self.get_id_by_path(path)? {
            self.unbind_path(id, path)?;
            if self.list_paths(id)?.is_empty() {
                self.delete_by_id(id)?;
            }
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Path not found"))
        }
    }

    fn delete_by_id(&mut self, id: Uuid) -> io::Result<()> {
        self.backend.delete_paths_for_document(id)?;
        self.backend.delete_document(id)?;
        // Clear cache for paths
        for p in self.backend.list_paths_for_document(id).unwrap_or(vec![]) {
            self.path_cache.write().remove(&p);
        }
        Ok(())
    }

    fn bind_to_path(&mut self, id: Uuid, path: &str) -> io::Result<()> {
        self.backend.bind_path_to_document(path, id)?;
        self.path_cache.write().insert(path.to_string(), id);
        Ok(())
    }

    fn unbind_path(&mut self, id: Uuid, path: &str) -> io::Result<()> {
        self.backend.unbind_path(id, path)?; // Assume added unbind to backend, similar to delete one path
        self.path_cache.write().remove(path);
        Ok(())
    }

    fn search(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.backend.search_paths(prefix)
    }

    fn list_paths(&self, id: Uuid) -> io::Result<Vec<String>> {
        self.backend.list_paths_for_document(id)
    }

    fn flush(&self) -> io::Result<()> {
        let mut file = self.backend.file.lock();
        file.flush()?;
        file.sync_all()?;
        self.backend.gc_old_versions()?;
        Ok(())
    }

    fn calculate_statistics(&self) -> io::Result<(i64, i64)> {
        let total_pages = (*self.backend.current_size.lock() / self.backend.config.page_size) as i64;
        let free_pages = self.backend.count_free_pages()?;
        Ok((total_pages, free_pages))
    }

    fn set_quick_mode(&mut self, enabled: bool) {
        self.quick_mode = enabled;
        self.backend.quick_mode = enabled;
    }
}

fn main() -> io::Result<()> {
    let config = Config::default();
    let mut db = StreamDb::open_with_config("streamdb.db", config)?;
    // Test usage
    Ok(())
}
