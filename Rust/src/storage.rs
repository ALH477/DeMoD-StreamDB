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
/// On-disk format version.
///
/// Bumped from 2 to 3 by the append-then-commit-header change. Version 2 files
/// are not readable, and are not worth migrating: in that format `flush()`
/// wrote the trie starting at offset 32, directly on top of the first
/// document, so every v2 file with at least one document in it is already
/// corrupt.
#[cfg(feature = "persistence")]
const VERSION: u32 = 3;

/// Size of one header slot.
///
/// Two alternating slots live at offset 0 and `HEADER_SLOT_SIZE`; document
/// data starts after both. Only 76 bytes of a slot are used; the rest is
/// reserved so the layout can grow without moving the data region.
#[cfg(feature = "persistence")]
const HEADER_SLOT_SIZE: u64 = 128;
/// Number of alternating header slots.
#[cfg(feature = "persistence")]
const HEADER_SLOTS: u64 = 2;
/// First byte available for document data.
#[cfg(feature = "persistence")]
const DATA_START: u64 = HEADER_SLOT_SIZE * HEADER_SLOTS;
/// Bytes at the start of a slot covered by its CRC.
#[cfg(feature = "persistence")]
const HEADER_CRC_COVERED: usize = 72;

/// A commit record: where the current trie and document index live.
///
/// Written last, and only after the data it points at has been `sync_data`d,
/// so a header that validates always describes a complete snapshot.
#[cfg(feature = "persistence")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct FileHeader {
    /// Monotonic commit counter. The newest valid slot wins.
    seq: u64,
    trie_offset: u64,
    trie_len: u64,
    trie_crc: u32,
    index_offset: u64,
    index_len: u64,
    index_crc: u32,
    /// End of everything this commit references; where the next write appends.
    data_end: u64,
}

#[cfg(feature = "persistence")]
impl FileHeader {
    fn encode(&self) -> [u8; HEADER_SLOT_SIZE as usize] {
        let mut buf = [0u8; HEADER_SLOT_SIZE as usize];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.seq.to_le_bytes());
        buf[16..24].copy_from_slice(&self.trie_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.trie_len.to_le_bytes());
        buf[32..36].copy_from_slice(&self.trie_crc.to_le_bytes());
        buf[40..48].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[48..56].copy_from_slice(&self.index_len.to_le_bytes());
        buf[56..60].copy_from_slice(&self.index_crc.to_le_bytes());
        buf[64..72].copy_from_slice(&self.data_end.to_le_bytes());
        let crc = compute_checksum(&buf[..HEADER_CRC_COVERED]);
        buf[72..76].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a slot, returning `None` for anything that isn't a valid commit.
    ///
    /// `file_len` bounds-checks the referenced regions: a header can be
    /// perfectly intact yet point past the end of a file that was truncated by
    /// a crash mid-flush. Such a slot is rejected so the caller falls back to
    /// the older slot, which is the whole point of keeping two.
    fn decode(buf: &[u8], file_len: u64) -> Option<Self> {
        if buf.len() < HEADER_SLOT_SIZE as usize {
            return None;
        }
        if buf[0..4] != MAGIC {
            return None;
        }
        if u32::from_le_bytes(buf[4..8].try_into().ok()?) != VERSION {
            return None;
        }
        let stored_crc = u32::from_le_bytes(buf[72..76].try_into().ok()?);
        if stored_crc != compute_checksum(&buf[..HEADER_CRC_COVERED]) {
            return None;
        }

        let h = Self {
            seq: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            trie_offset: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            trie_len: u64::from_le_bytes(buf[24..32].try_into().ok()?),
            trie_crc: u32::from_le_bytes(buf[32..36].try_into().ok()?),
            index_offset: u64::from_le_bytes(buf[40..48].try_into().ok()?),
            index_len: u64::from_le_bytes(buf[48..56].try_into().ok()?),
            index_crc: u32::from_le_bytes(buf[56..60].try_into().ok()?),
            data_end: u64::from_le_bytes(buf[64..72].try_into().ok()?),
        };

        if h.trie_offset.checked_add(h.trie_len)? > file_len {
            return None;
        }
        if h.index_offset.checked_add(h.index_len)? > file_len {
            return None;
        }
        if h.data_end > file_len {
            return None;
        }
        Some(h)
    }
}

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
    /// Sequence number of the last committed header.
    seq: AtomicU64,
    /// Backing file path, needed to rewrite it during [`FileBackend::compact`].
    path: std::path::PathBuf,
    config: Config,
    #[cfg(not(target_arch = "wasm32"))]
    mmap: RwLock<Option<memmap2::MmapMut>>,
}

#[cfg(feature = "persistence")]
impl FileBackend {
    /// Get the configuration
    pub fn config(&self) -> &Config {
        &self.config
    }
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
            next_offset: AtomicU64::new(DATA_START),
            seq: AtomicU64::new(0),
            path: path.to_path_buf(),
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

        // Slot 1 holds the initial (empty) commit; slot 0 is left zeroed, so
        // its invalid magic makes `load_file` ignore it.
        let header = FileHeader {
            seq: 1,
            data_end: DATA_START,
            ..Default::default()
        };

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&[0u8; HEADER_SLOT_SIZE as usize])?;
        file.write_all(&header.encode())?;
        file.flush()?;
        file.sync_all()?;

        self.seq.store(header.seq, Ordering::SeqCst);
        self.next_offset.store(DATA_START, Ordering::SeqCst);

        Ok(())
    }

    fn load_file(&self) -> Result<Trie> {
        let mut file = self.file.lock();
        let file_len = file.metadata()?.len();

        // Read both header slots up front.
        let mut slots = [0u8; DATA_START as usize];
        let readable = file_len.min(DATA_START) as usize;
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut slots[..readable])?;

        // Newest valid commit first; a partially-written newer slot falls back
        // to the older one.
        let mut candidates: Vec<FileHeader> = (0..HEADER_SLOTS)
            .filter_map(|i| {
                let start = (i * HEADER_SLOT_SIZE) as usize;
                let end = start + HEADER_SLOT_SIZE as usize;
                if end > readable {
                    return None;
                }
                FileHeader::decode(&slots[start..end], file_len)
            })
            .collect();
        candidates.sort_by(|a, b| b.seq.cmp(&a.seq));

        if candidates.is_empty() {
            return Err(Error::Corrupted(
                "No valid header slot (not a StreamDB file, or both commits damaged)".into(),
            ));
        }

        let mut last_err = None;
        for header in &candidates {
            match self.load_commit(&mut file, header) {
                Ok(trie) => {
                    self.seq.store(header.seq, Ordering::SeqCst);
                    self.next_offset
                        .store(header.data_end.max(DATA_START), Ordering::SeqCst);
                    return Ok(trie);
                }
                Err(e) => {
                    log::warn!(
                        "StreamDB: header slot seq={} unusable ({}), trying older commit",
                        header.seq,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| Error::Corrupted("No usable commit".into())))
    }

    /// Load the trie + document index described by one header.
    fn load_commit(&self, file: &mut File, header: &FileHeader) -> Result<Trie> {
        // An empty database commits a zero-length trie.
        if header.trie_len == 0 {
            self.documents.write().clear();
            self.total_size.store(0, Ordering::Relaxed);
            return Ok(Trie::new());
        }

        let mut trie_data = vec![0u8; header.trie_len as usize];
        file.seek(SeekFrom::Start(header.trie_offset))?;
        file.read_exact(&mut trie_data)?;
        if compute_checksum(&trie_data) != header.trie_crc {
            return Err(Error::Corrupted("Trie checksum mismatch".into()));
        }
        let trie: Trie = bincode::deserialize(&trie_data)
            .map_err(|e| Error::Corrupted(format!("Failed to deserialize trie: {}", e)))?;

        let mut index = vec![0u8; header.index_len as usize];
        file.seek(SeekFrom::Start(header.index_offset))?;
        file.read_exact(&mut index)?;
        if compute_checksum(&index) != header.index_crc {
            return Err(Error::Corrupted("Document index checksum mismatch".into()));
        }

        let mut cursor = std::io::Cursor::new(&index);
        let doc_count = cursor.read_u64::<LittleEndian>()?;
        let mut docs = HashMap::with_capacity(doc_count as usize);
        let mut total_size = 0u64;

        for _ in 0..doc_count {
            let mut id_bytes = [0u8; 16];
            cursor.read_exact(&mut id_bytes)?;
            let id = Uuid::from_bytes(id_bytes);

            let offset = cursor.read_u64::<LittleEndian>()?;
            let size = cursor.read_u32::<LittleEndian>()?;
            let checksum = cursor.read_u32::<LittleEndian>()?;

            docs.insert(id, DocumentMeta { offset, size, checksum });
            total_size += size as u64;
        }

        *self.documents.write() = docs;
        self.total_size.store(total_size, Ordering::Relaxed);

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

    /// Rewrite the file keeping only live documents.
    ///
    /// The store is append-only: deleted documents and every superseded
    /// trie/index blob stay on disk until this runs. A long-lived process that
    /// flushes periodically grows without bound otherwise.
    ///
    /// Document UUIDs are preserved, so the caller's `trie` stays valid and is
    /// re-committed as-is. Writes into a sibling temp file and renames over the
    /// original, so an interrupted compaction leaves the existing database
    /// untouched.
    pub fn compact(&self, trie: &Trie) -> Result<()> {
        // Drop the mapping first: it pins pages of the inode we're replacing.
        #[cfg(not(target_arch = "wasm32"))]
        {
            *self.mmap.write() = None;
        }

        let tmp_path = {
            let mut p = self.path.clone().into_os_string();
            p.push(".compact");
            std::path::PathBuf::from(p)
        };

        let mut file = self.file.lock();

        let mut out = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Reserve both header slots; they are written last.
        out.write_all(&[0u8; DATA_START as usize])?;

        let old_docs: Vec<(Uuid, DocumentMeta)> = self
            .documents
            .read()
            .iter()
            .map(|(id, meta)| (*id, meta.clone()))
            .collect();

        let mut new_docs = HashMap::with_capacity(old_docs.len());
        let mut total_size = 0u64;
        let mut offset = DATA_START;

        for (id, meta) in &old_docs {
            let mut data = vec![0u8; meta.size as usize];
            file.seek(SeekFrom::Start(meta.offset))?;
            file.read_exact(&mut data)?;
            if compute_checksum(&data) != meta.checksum {
                return Err(Error::Corrupted(format!(
                    "Document {} failed checksum during compaction; aborting",
                    id
                )));
            }

            out.write_u32::<LittleEndian>(meta.size)?;
            out.write_u32::<LittleEndian>(meta.checksum)?;
            out.write_all(&data)?;

            new_docs.insert(
                *id,
                DocumentMeta {
                    offset: offset + 8,
                    size: meta.size,
                    checksum: meta.checksum,
                },
            );
            total_size += meta.size as u64;
            offset += meta.size as u64 + 8;
        }

        // One fresh commit at the head of the new file.
        let trie_data = bincode::serialize(trie)?;
        let trie_crc = compute_checksum(&trie_data);

        let mut index = Vec::with_capacity(8 + new_docs.len() * 32);
        index.write_u64::<LittleEndian>(new_docs.len() as u64)?;
        for (id, meta) in new_docs.iter() {
            index.write_all(id.as_bytes())?;
            index.write_u64::<LittleEndian>(meta.offset)?;
            index.write_u32::<LittleEndian>(meta.size)?;
            index.write_u32::<LittleEndian>(meta.checksum)?;
        }
        let index_crc = compute_checksum(&index);

        let trie_offset = offset;
        let index_offset = trie_offset + trie_data.len() as u64;
        out.write_all(&trie_data)?;
        out.write_all(&index)?;

        let header = FileHeader {
            seq: 1,
            trie_offset,
            trie_len: trie_data.len() as u64,
            trie_crc,
            index_offset,
            index_len: index.len() as u64,
            index_crc,
            data_end: index_offset + index.len() as u64,
        };
        out.seek(SeekFrom::Start((header.seq % HEADER_SLOTS) * HEADER_SLOT_SIZE))?;
        out.write_all(&header.encode())?;
        out.flush()?;
        out.sync_all()?;

        std::fs::rename(&tmp_path, &self.path)?;

        *file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        *self.documents.write() = new_docs;
        self.total_size.store(total_size, Ordering::SeqCst);
        self.next_offset.store(header.data_end, Ordering::SeqCst);
        self.seq.store(header.seq, Ordering::SeqCst);

        drop(file);

        #[cfg(not(target_arch = "wasm32"))]
        if self.config.use_mmap {
            self.setup_mmap()?;
        }

        Ok(())
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
    
    /// Commit the trie and document index.
    ///
    /// Append-then-commit-header: both blobs are appended *past* every
    /// document, fsynced, and only then does the fixed header slot start
    /// pointing at them. Nothing ever overwrites live data, so a crash at any
    /// point leaves the previous commit intact and loses at most the last
    /// `flush()`.
    ///
    /// The previous commit's trie/index become garbage — space is reclaimed by
    /// [`FileBackend::compact`], not here.
    fn flush(&self, trie: &Trie) -> Result<()> {
        let trie_data = bincode::serialize(trie)?;
        let trie_crc = compute_checksum(&trie_data);

        // Serialise the document index into one blob so it gets a single CRC.
        let index = {
            let docs = self.documents.read();
            let mut buf = Vec::with_capacity(8 + docs.len() * 32);
            buf.write_u64::<LittleEndian>(docs.len() as u64)?;
            for (id, meta) in docs.iter() {
                buf.write_all(id.as_bytes())?;
                buf.write_u64::<LittleEndian>(meta.offset)?;
                buf.write_u32::<LittleEndian>(meta.size)?;
                buf.write_u32::<LittleEndian>(meta.checksum)?;
            }
            buf
        };
        let index_crc = compute_checksum(&index);

        let mut file = self.file.lock();

        // Reserve past every document, including any a concurrent `write()`
        // has already allocated but not yet written.
        let total = trie_data.len() as u64 + index.len() as u64;
        let trie_offset = self.next_offset.fetch_add(total, Ordering::SeqCst);
        let index_offset = trie_offset + trie_data.len() as u64;

        file.seek(SeekFrom::Start(trie_offset))?;
        file.write_all(&trie_data)?;
        file.write_all(&index)?;
        file.flush()?;
        // Ordering is the whole guarantee: the data must be durable BEFORE the
        // header referencing it, or a crash can leave a valid header pointing
        // at bytes that were never written.
        file.sync_data()?;

        let header = FileHeader {
            seq: self.seq.load(Ordering::SeqCst) + 1,
            trie_offset,
            trie_len: trie_data.len() as u64,
            trie_crc,
            index_offset,
            index_len: index.len() as u64,
            index_crc,
            data_end: index_offset + index.len() as u64,
        };

        file.seek(SeekFrom::Start((header.seq % HEADER_SLOTS) * HEADER_SLOT_SIZE))?;
        file.write_all(&header.encode())?;
        file.flush()?;
        file.sync_all()?;

        self.seq.store(header.seq, Ordering::SeqCst);

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

    // ========================================================================
    // Persistence regressions
    //
    // Before the append-then-commit-header change, `flush()` seeked to 0 and
    // wrote the trie starting at offset 32 — the same offset documents were
    // allocated from. Every one of these tests failed (or could not have been
    // written) against that format.
    // ========================================================================

    /// Deterministic xorshift, so a failure here reproduces exactly.
    #[cfg(feature = "persistence")]
    struct Rng(u64);

    #[cfg(feature = "persistence")]
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// The index must survive a reopen, not just the document bytes: a store
    /// whose keys are gone is useless even if the values are intact.
    #[cfg(feature = "persistence")]
    #[test]
    fn suffix_search_works_after_reopen() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("suffix.db");

        {
            let db = crate::StreamDb::open(&path, Config::default()).unwrap();
            db.insert(b"7.player", b"player seven").unwrap();
            db.insert(b"42.player", b"player forty-two").unwrap();
            db.insert(b"3.clan", b"a clan").unwrap();
            db.flush().unwrap();
        }

        let db = crate::StreamDb::open(&path, Config::default()).unwrap();
        let players = db.suffix_search(b".player").unwrap();
        assert_eq!(players.len(), 2, "suffix index lost across reopen");

        let mut keys: Vec<_> = players.iter().map(|r| r.key.clone()).collect();
        keys.sort();
        assert_eq!(keys, vec![b"42.player".to_vec(), b"7.player".to_vec()]);

        assert_eq!(db.get(b"3.clan").unwrap(), Some(b"a clan".to_vec()));
    }

    /// Enough data that the trie is far larger than the 32-byte region the old
    /// format collided in — this is the case the old code could never survive.
    #[cfg(feature = "persistence")]
    #[test]
    fn many_large_documents_survive_reopen() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("bulk.db");

        const COUNT: usize = 200;
        const SIZE: usize = 4096;

        {
            let db = crate::StreamDb::open(&path, Config::default()).unwrap();
            for i in 0..COUNT {
                let value = vec![(i % 251) as u8; SIZE];
                db.insert(format!("{}.blob", i).as_bytes(), &value).unwrap();
            }
            db.flush().unwrap();
        }

        let db = crate::StreamDb::open(&path, Config::default()).unwrap();
        for i in 0..COUNT {
            let got = db
                .get(format!("{}.blob", i).as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("document {} missing after reopen", i));
            assert_eq!(got.len(), SIZE);
            assert!(
                got.iter().all(|&b| b == (i % 251) as u8),
                "document {} came back with wrong contents",
                i
            );
        }
        assert_eq!(db.suffix_search(b".blob").unwrap().len(), COUNT);
    }

    /// Crash between appending a commit's data and writing its header.
    ///
    /// The truncated file still carries the *newer* header slot, which points
    /// past EOF. It must be rejected in favour of the previous commit rather
    /// than failing the open outright.
    #[cfg(feature = "persistence")]
    #[test]
    fn crash_before_header_commit_keeps_previous_snapshot() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("crash.db");

        // Commit 1.
        {
            let db = crate::StreamDb::open(&path, Config::default()).unwrap();
            db.insert(b"survivor", b"first").unwrap();
            db.flush().unwrap();
        }
        let after_commit1 = std::fs::metadata(&path).unwrap().len();

        // Commit 2.
        {
            let db = crate::StreamDb::open(&path, Config::default()).unwrap();
            db.insert(b"casualty", b"second").unwrap();
            db.flush().unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > after_commit1);

        // Lose everything commit 2 appended, keeping both header slots.
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(after_commit1)
            .unwrap();

        let db = crate::StreamDb::open(&path, Config::default()).unwrap();
        assert_eq!(
            db.get(b"survivor").unwrap(),
            Some(b"first".to_vec()),
            "previous commit should be intact after a torn flush"
        );
        assert_eq!(
            db.get(b"casualty").unwrap(),
            None,
            "the uncommitted write should be gone, not half-present"
        );

        // And the recovered database must still be usable.
        db.insert(b"after-recovery", b"third").unwrap();
        db.flush().unwrap();
        drop(db);

        let db = crate::StreamDb::open(&path, Config::default()).unwrap();
        assert_eq!(db.get(b"after-recovery").unwrap(), Some(b"third".to_vec()));
        assert_eq!(db.get(b"survivor").unwrap(), Some(b"first".to_vec()));
    }

    /// A header slot torn mid-write fails its CRC; the older slot carries on.
    #[cfg(feature = "persistence")]
    #[test]
    fn torn_header_falls_back_to_previous_commit() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("torn.db");

        {
            let db = crate::StreamDb::open(&path, Config::default()).unwrap();
            db.insert(b"keep", b"one").unwrap();
            db.flush().unwrap();
            db.insert(b"lose", b"two").unwrap();
            db.flush().unwrap();
        }

        // Find the newest slot and scribble on it.
        let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let len = file.metadata().unwrap().len();
        let mut slots = [0u8; DATA_START as usize];
        file.read_exact(&mut slots).unwrap();

        let newest = (0..HEADER_SLOTS)
            .filter_map(|i| {
                let s = (i * HEADER_SLOT_SIZE) as usize;
                FileHeader::decode(&slots[s..s + HEADER_SLOT_SIZE as usize], len).map(|h| (i, h.seq))
            })
            .max_by_key(|(_, seq)| *seq)
            .expect("both slots should be valid here")
            .0;

        file.seek(SeekFrom::Start(newest * HEADER_SLOT_SIZE + 16)).unwrap();
        file.write_all(&[0xAB; 8]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let db = crate::StreamDb::open(&path, Config::default()).unwrap();
        assert_eq!(db.get(b"keep").unwrap(), Some(b"one".to_vec()));
        assert_eq!(db.get(b"lose").unwrap(), None);
    }

    /// Random insert/delete/flush/reopen cycles against a `HashMap` oracle.
    #[cfg(feature = "persistence")]
    #[test]
    fn random_operations_match_hashmap_across_reopens() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("fuzz.db");

        let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut rng = Rng(0x5EED_1234_ABCD_0001);

        for round in 0..12 {
            let db = crate::StreamDb::open(&path, Config::default()).unwrap();

            for _ in 0..40 {
                let key = format!("{}.k", rng.below(50)).into_bytes();
                match rng.below(4) {
                    0 => {
                        db.delete(&key).unwrap();
                        model.remove(&key);
                    }
                    _ => {
                        let len = 1 + rng.below(64) as usize;
                        let byte = rng.next() as u8;
                        let value = vec![byte; len];
                        db.insert(&key, &value).unwrap();
                        model.insert(key, value);
                    }
                }
            }

            db.flush().unwrap();
            drop(db);

            let db = crate::StreamDb::open(&path, Config::default()).unwrap();
            for (key, expected) in &model {
                assert_eq!(
                    db.get(key).unwrap().as_ref(),
                    Some(expected),
                    "round {}: mismatch for {:?}",
                    round,
                    String::from_utf8_lossy(key)
                );
            }
            assert_eq!(
                db.suffix_search(b".k").unwrap().len(),
                model.len(),
                "round {}: key count diverged from model",
                round
            );
        }
    }

    /// Compaction must shrink the file, drop deleted documents, and leave every
    /// live key readable — including after a reopen.
    #[cfg(feature = "persistence")]
    #[test]
    fn compact_reclaims_space_and_preserves_live_data() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("compact.db");

        let (backend, mut trie) = FileBackend::open(&path, &Config::default()).unwrap();

        // Churn: write 60 documents, then delete two thirds of them, flushing
        // each round so superseded trie/index blobs accumulate.
        let mut live = Vec::new();
        for i in 0..60u32 {
            let id = backend.write(&vec![i as u8; 2048]).unwrap();
            trie = trie.insert(format!("{}.doc", i).as_bytes(), id);
            if i % 3 == 0 {
                live.push((i, id));
            }
            backend.flush(&trie).unwrap();
        }
        for i in 0..60u32 {
            if i % 3 != 0 {
                let id = trie.get(format!("{}.doc", i).as_bytes()).unwrap();
                backend.delete(id).unwrap();
                trie = trie
                    .remove(format!("{}.doc", i).as_bytes())
                    .expect("key just inserted must be removable");
            }
        }
        backend.flush(&trie).unwrap();

        let before = std::fs::metadata(&path).unwrap().len();
        backend.compact(&trie).unwrap();
        let after = std::fs::metadata(&path).unwrap().len();

        assert!(
            after < before,
            "compaction did not reclaim anything ({} -> {} bytes)",
            before,
            after
        );

        // Readable immediately through the compacted handle...
        for (i, id) in &live {
            let data = backend.read(*id).unwrap();
            assert_eq!(data.len(), 2048);
            assert!(data.iter().all(|&b| b == *i as u8));
        }
        drop(backend);

        // ...and after reopening the rewritten file.
        let (backend, trie2) = FileBackend::open(&path, &Config::default()).unwrap();
        for (i, id) in &live {
            assert_eq!(trie2.get(format!("{}.doc", i).as_bytes()), Some(*id));
            assert!(backend.read(*id).unwrap().iter().all(|&b| b == *i as u8));
        }
        assert_eq!(trie2.get(b"1.doc"), None, "deleted key came back");
    }
}
