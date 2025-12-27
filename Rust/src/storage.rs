//! Storage backends for StreamDB.
//!
//! This module provides the `Backend` trait and implementations for
//! in-memory and file-based storage.

use crate::error::{Error, Result};
use crate::trie::Trie;
use crate::{CacheStats, Config};

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;
use std::collections::HashMap;
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "persistence")]
use std::fs::{File, OpenOptions};
#[cfg(feature = "persistence")]
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(feature = "persistence")]
use std::path::Path;
#[cfg(feature = "persistence")]
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
#[cfg(feature = "persistence")]
use crc32fast::Hasher as Crc32Hasher;

/// Database statistics
#[derive(Debug, Clone, Default)]
pub struct Stats {
    /// Number of keys in the database
    pub key_count: usize,
    /// Total size of all values in bytes
    pub total_size: u64,
    /// Cache statistics
    pub cache_stats: CacheStats,
    /// Whether there are unflushed changes
    pub is_dirty: bool,
}

/// Backend statistics (internal)
#[derive(Debug, Clone, Default)]
pub struct BackendStats {
    /// Total size of all stored values
    pub total_size: u64,
    /// Number of documents stored
    pub doc_count: u64,
}

/// Storage backend trait
///
/// Implementations provide the actual storage mechanism for document data.
pub trait Backend: Send + Sync + Any {
    /// Write data and return a unique ID
    fn write(&self, data: &[u8]) -> Result<Uuid>;
    
    /// Read data by ID
    fn read(&self, id: Uuid) -> Result<Vec<u8>>;
    
    /// Delete data by ID
    fn delete(&self, id: Uuid) -> Result<()>;
    
    /// Flush trie and data to persistent storage
    fn flush(&self, trie: &Trie) -> Result<()>;
    
    /// Get backend statistics
    fn stats(&self) -> Result<BackendStats>;
    
    /// Downcast to concrete type
    fn as_any(&self) -> &dyn Any;
}

/// In-memory storage backend
///
/// Stores all data in memory. Data is lost when the database is closed.
pub struct MemoryBackend {
    documents: RwLock<HashMap<Uuid, Vec<u8>>>,
    total_size: AtomicU64,
}

impl MemoryBackend {
    /// Create a new in-memory backend
    pub fn new() -> Self {
        Self {
            documents: RwLock::new(HashMap::new()),
            total_size: AtomicU64::new(0),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend for MemoryBackend {
    fn write(&self, data: &[u8]) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let size = data.len() as u64;
        
        let mut docs = self.documents.write();
        
        // If updating, subtract old size
        if let Some(old) = docs.get(&id) {
            self.total_size.fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
        
        docs.insert(id, data.to_vec());
        self.total_size.fetch_add(size, Ordering::Relaxed);
        
        Ok(id)
    }
    
    fn read(&self, id: Uuid) -> Result<Vec<u8>> {
        let docs = self.documents.read();
        docs.get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("Document not found: {}", id)))
    }
    
    fn delete(&self, id: Uuid) -> Result<()> {
        let mut docs = self.documents.write();
        
        if let Some(old) = docs.remove(&id) {
            self.total_size.fetch_sub(old.len() as u64, Ordering::Relaxed);
            Ok(())
        } else {
            Err(Error::NotFound(format!("Document not found: {}", id)))
        }
    }
    
    fn flush(&self, _trie: &Trie) -> Result<()> {
        // Memory backend doesn't persist
        Ok(())
    }
    
    fn stats(&self) -> Result<BackendStats> {
        let docs = self.documents.read();
        Ok(BackendStats {
            total_size: self.total_size.load(Ordering::Relaxed),
            doc_count: docs.len() as u64,
        })
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ============================================================================
// File Backend (persistence feature)
// ============================================================================

#[cfg(feature = "persistence")]
const MAGIC: [u8; 4] = [0x53, 0x54, 0x44, 0x42]; // "STDB"
#[cfg(feature = "persistence")]
const VERSION: u32 = 2;
#[cfg(feature = "persistence")]
const HEADER_SIZE: u64 = 32;

/// File-based storage backend
///
/// Provides persistent storage with crash recovery.
#[cfg(feature = "persistence")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistence")))]
pub struct FileBackend {
    file: Mutex<File>,
    documents: RwLock<HashMap<Uuid, DocumentMeta>>,
    total_size: AtomicU64,
    next_offset: AtomicU64,
    config: Config,
    #[cfg(not(target_arch = "wasm32"))]
    mmap: RwLock<Option<memmap2::MmapMut>>,
}

#[cfg(feature = "persistence")]
#[derive(Clone, Debug)]
struct DocumentMeta {
    offset: u64,
    size: u32,
    checksum: u32,
}

#[cfg(feature = "persistence")]
impl FileBackend {
    /// Open or create a database file
    pub fn open(path: &Path, config: &Config) -> Result<(Self, Trie)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        
        let backend = Self {
            file: Mutex::new(file),
            documents: RwLock::new(HashMap::new()),
            total_size: AtomicU64::new(0),
            next_offset: AtomicU64::new(HEADER_SIZE),
            config: config.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            mmap: RwLock::new(None),
        };
        
        // Load existing data or initialize new file
        let trie = if file_size == 0 {
            backend.initialize_file()?;
            Trie::new()
        } else {
            backend.load_file()?
        };
        
        // Setup mmap if enabled
        #[cfg(not(target_arch = "wasm32"))]
        if config.use_mmap {
            backend.setup_mmap()?;
        }
        
        Ok((backend, trie))
    }
    
    fn initialize_file(&self) -> Result<()> {
        let mut file = self.file.lock();
        
        // Write header
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&MAGIC)?;
        file.write_u32::<LittleEndian>(VERSION)?;
        
        // Reserved space
        let reserved = [0u8; 24];
        file.write_all(&reserved)?;
        
        file.flush()?;
        
        Ok(())
    }
    
    fn load_file(&self) -> Result<Trie> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;
        
        // Verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(Error::Corrupted("Invalid file magic".into()));
        }
        
        // Read version
        let version = file.read_u32::<LittleEndian>()?;
        if version != VERSION {
            return Err(Error::Corrupted(format!(
                "Unsupported version: {} (expected {})",
                version, VERSION
            )));
        }
        
        // Skip reserved
        file.seek(SeekFrom::Current(24))?;
        
        // Read trie length
        let trie_len = match file.read_u64::<LittleEndian>() {
            Ok(len) => len,
            Err(_) => return Ok(Trie::new()), // Empty database
        };
        
        if trie_len == 0 {
            return Ok(Trie::new());
        }
        
        // Read and verify trie checksum
        let expected_checksum = file.read_u32::<LittleEndian>()?;
        
        // Read trie data
        let mut trie_data = vec![0u8; trie_len as usize];
        file.read_exact(&mut trie_data)?;
        
        // Verify checksum
        let actual_checksum = compute_checksum(&trie_data);
        if actual_checksum != expected_checksum {
            return Err(Error::Corrupted("Trie checksum mismatch".into()));
        }
        
        // Deserialize trie
        let trie: Trie = bincode::deserialize(&trie_data)
            .map_err(|e| Error::Corrupted(format!("Failed to deserialize trie: {}", e)))?;
        
        // Read document index
        let doc_count = file.read_u64::<LittleEndian>().unwrap_or(0);
        let mut docs = self.documents.write();
        let mut total_size = 0u64;
        let mut max_offset = HEADER_SIZE;
        
        for _ in 0..doc_count {
            let mut id_bytes = [0u8; 16];
            if file.read_exact(&mut id_bytes).is_err() {
                break;
            }
            let id = Uuid::from_bytes(id_bytes);
            
            let offset = file.read_u64::<LittleEndian>()?;
            let size = file.read_u32::<LittleEndian>()?;
            let checksum = file.read_u32::<LittleEndian>()?;
            
            docs.insert(id, DocumentMeta { offset, size, checksum });
            total_size += size as u64;
            max_offset = max_offset.max(offset + size as u64);
        }
        
        drop(docs);
        
        self.total_size.store(total_size, Ordering::Relaxed);
        self.next_offset.store(max_offset, Ordering::Relaxed);
        
        Ok(trie)
    }
    
    #[cfg(not(target_arch = "wasm32"))]
    fn setup_mmap(&self) -> Result<()> {
        let file = self.file.lock();
        let len = file.metadata()?.len();
        
        if len > 0 {
            let mmap = unsafe { memmap2::MmapOptions::new().map_mut(&*file)? };
            *self.mmap.write() = Some(mmap);
        }
        
        Ok(())
    }
    
    fn compute_document_checksum(data: &[u8]) -> u32 {
        compute_checksum(data)
    }
}

#[cfg(feature = "persistence")]
impl Backend for FileBackend {
    fn write(&self, data: &[u8]) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let size = data.len() as u32;
        let checksum = Self::compute_document_checksum(data);
        
        // Allocate space
        let offset = self.next_offset.fetch_add(size as u64 + 8, Ordering::SeqCst);
        
        // Write to file
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset))?;
            file.write_u32::<LittleEndian>(size)?;
            file.write_u32::<LittleEndian>(checksum)?;
            file.write_all(data)?;
        }
        
        // Update index
        {
            let mut docs = self.documents.write();
            docs.insert(id, DocumentMeta {
                offset: offset + 8, // Skip size/checksum header
                size,
                checksum,
            });
        }
        
        self.total_size.fetch_add(size as u64, Ordering::Relaxed);
        
        Ok(id)
    }
    
    fn read(&self, id: Uuid) -> Result<Vec<u8>> {
        let meta = {
            let docs = self.documents.read();
            docs.get(&id)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("Document not found: {}", id)))?
        };
        
        // Try mmap first
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mmap_guard = self.mmap.read();
            if let Some(mmap) = mmap_guard.as_ref() {
                let start = meta.offset as usize;
                let end = start + meta.size as usize;
                
                if end <= mmap.len() {
                    let data = mmap[start..end].to_vec();
                    
                    // Verify checksum
                    let actual = compute_checksum(&data);
                    if actual != meta.checksum {
                        return Err(Error::Corrupted("Document checksum mismatch".into()));
                    }
                    
                    return Ok(data);
                }
            }
        }
        
        // Fallback to file read
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(meta.offset))?;
        
        let mut data = vec![0u8; meta.size as usize];
        file.read_exact(&mut data)?;
        
        // Verify checksum
        let actual = compute_checksum(&data);
        if actual != meta.checksum {
            return Err(Error::Corrupted("Document checksum mismatch".into()));
        }
        
        Ok(data)
    }
    
    fn delete(&self, id: Uuid) -> Result<()> {
        let mut docs = self.documents.write();
        
        if let Some(meta) = docs.remove(&id) {
            self.total_size.fetch_sub(meta.size as u64, Ordering::Relaxed);
            // Note: Space is not reclaimed (append-only for simplicity)
            // A compaction pass would be needed for production use
            Ok(())
        } else {
            Err(Error::NotFound(format!("Document not found: {}", id)))
        }
    }
    
    fn flush(&self, trie: &Trie) -> Result<()> {
        // Serialize trie
        let trie_data = bincode::serialize(trie)?;
        let trie_checksum = compute_checksum(&trie_data);
        
        // Build document index
        let docs = self.documents.read();
        let doc_index: Vec<_> = docs.iter()
            .map(|(id, meta)| (*id, meta.clone()))
            .collect();
        drop(docs);
        
        // Write to file
        let mut file = self.file.lock();
        
        // Write header
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&MAGIC)?;
        file.write_u32::<LittleEndian>(VERSION)?;
        
        // Reserved
        file.write_all(&[0u8; 24])?;
        
        // Write trie
        file.write_u64::<LittleEndian>(trie_data.len() as u64)?;
        file.write_u32::<LittleEndian>(trie_checksum)?;
        file.write_all(&trie_data)?;
        
        // Write document index
        file.write_u64::<LittleEndian>(doc_index.len() as u64)?;
        for (id, meta) in &doc_index {
            file.write_all(id.as_bytes())?;
            file.write_u64::<LittleEndian>(meta.offset)?;
            file.write_u32::<LittleEndian>(meta.size)?;
            file.write_u32::<LittleEndian>(meta.checksum)?;
        }
        
        file.flush()?;
        file.sync_all()?;
        
        Ok(())
    }
    
    fn stats(&self) -> Result<BackendStats> {
        let docs = self.documents.read();
        Ok(BackendStats {
            total_size: self.total_size.load(Ordering::Relaxed),
            doc_count: docs.len() as u64,
        })
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(feature = "persistence")]
fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_memory_backend_basic() {
        let backend = MemoryBackend::new();
        
        // Write
        let data = b"hello world";
        let id = backend.write(data).unwrap();
        
        // Read
        let retrieved = backend.read(id).unwrap();
        assert_eq!(retrieved, data);
        
        // Stats
        let stats = backend.stats().unwrap();
        assert_eq!(stats.total_size, data.len() as u64);
        assert_eq!(stats.doc_count, 1);
        
        // Delete
        backend.delete(id).unwrap();
        assert!(backend.read(id).is_err());
        
        let stats = backend.stats().unwrap();
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.doc_count, 0);
    }
    
    #[test]
    fn test_memory_backend_multiple() {
        let backend = MemoryBackend::new();
        
        let id1 = backend.write(b"data1").unwrap();
        let id2 = backend.write(b"data2").unwrap();
        let id3 = backend.write(b"data3").unwrap();
        
        assert_eq!(backend.read(id1).unwrap(), b"data1");
        assert_eq!(backend.read(id2).unwrap(), b"data2");
        assert_eq!(backend.read(id3).unwrap(), b"data3");
        
        let stats = backend.stats().unwrap();
        assert_eq!(stats.doc_count, 3);
    }
    
    #[cfg(feature = "persistence")]
    #[test]
    fn test_file_backend_basic() {
        use tempfile::tempdir;
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        // Create and write
        {
            let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();
            
            let id = backend.write(b"hello world").unwrap();
            let retrieved = backend.read(id).unwrap();
            assert_eq!(retrieved, b"hello world");
            
            let trie = trie.insert(b"test", id);
            backend.flush(&trie).unwrap();
        }
        
        // Reopen and verify
        {
            let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();
            
            let id = trie.get(b"test").unwrap();
            let retrieved = backend.read(id).unwrap();
            assert_eq!(retrieved, b"hello world");
        }
    }
    
    #[cfg(feature = "persistence")]
    #[test]
    fn test_file_backend_persistence() {
        use tempfile::tempdir;
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.db");
        
        let id1;
        let id2;
        
        // First session: write data
        {
            let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();
            
            id1 = backend.write(b"value1").unwrap();
            id2 = backend.write(b"value2").unwrap();
            
            let trie = trie.insert(b"key1", id1).insert(b"key2", id2);
            backend.flush(&trie).unwrap();
        }
        
        // Second session: verify data
        {
            let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();
            
            assert_eq!(trie.get(b"key1"), Some(id1));
            assert_eq!(trie.get(b"key2"), Some(id2));
            
            assert_eq!(backend.read(id1).unwrap(), b"value1");
            assert_eq!(backend.read(id2).unwrap(), b"value2");
        }
    }
}
