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
#[cfg(all(feature = "persistence", not(target_arch = "wasm32")))]
use fs2::FileExt;

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

        // Every write creates a fresh document; key-level "update" happens at
        // the StreamDb layer by pointing the key at a new ID.
        let mut docs = self.documents.write();
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
    /// Serialises `flush()`: two concurrent commits must not load the same
    /// `seq` and both write to the same header slot, the second clobbering
    /// the first.
    flush_lock: Mutex<()>,
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

        // Exclusive advisory lock: two handles on the same path keep
        // independent `next_offset`s and would interleave appends, silently
        // corrupting the store. Held for the lifetime of the `File`.
        #[cfg(not(target_arch = "wasm32"))]
        file.try_lock_exclusive().map_err(|e| {
            Error::ResourceLimit(format!(
                "cannot lock {} (already open in this or another process?): {}",
                path.display(),
                e
            ))
        })?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        
        let backend = Self {
            file: Mutex::new(file),
            documents: RwLock::new(HashMap::new()),
            total_size: AtomicU64::new(0),
            next_offset: AtomicU64::new(DATA_START),
            seq: AtomicU64::new(0),
            flush_lock: Mutex::new(()),
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
    ///
    /// Concurrency: the document map write-lock and the file lock are held for
    /// the whole operation (lock order everywhere is documents -> file), so
    /// `write()`/`delete()` block until the rename completes. A `write()` can
    /// therefore never land at a stale offset inside the new inode, and a
    /// `delete()` issued mid-compaction is applied to the new map rather than
    /// being silently undone by it.
    pub fn compact(&self, trie: &Trie) -> Result<()> {
        // Drop the mapping first: it pins pages of the inode we're replacing.
        #[cfg(not(target_arch = "wasm32"))]
        {
            *self.mmap.write() = None;
        }

        // Hold both locks for the entire compaction, iterating the LIVE map
        // (not a stale snapshot) so deletes that landed before this call are
        // honoured and writers/deleters block until the new file is in place.
        let mut docs_guard = self.documents.write();
        let mut file = self.file.lock();

        // Documents referenced by the committed trie. Anything else in the
        // index is an orphan (superseded update whose cleanup failed, or a
        // delete that errored mid-way) and is reclaimed here.
        let mut live_ids: std::collections::HashSet<Uuid> =
            std::collections::HashSet::with_capacity(trie.len());
        trie.for_each(&mut |_, id| {
            live_ids.insert(id);
            true
        });

        let tmp_path = {
            let mut p = self.path.clone().into_os_string();
            p.push(".compact");
            std::path::PathBuf::from(p)
        };

        let mut out = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Reserve both header slots; they are written last.
        out.write_all(&[0u8; DATA_START as usize])?;

        let mut new_docs = HashMap::with_capacity(docs_guard.len());
        let mut total_size = 0u64;
        let mut offset = DATA_START;

        for (id, meta) in docs_guard.iter() {
            if !live_ids.contains(id) {
                continue; // orphan: reclaim by omission
            }
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

        let index = serialize_index(&new_docs)?;
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
        drop(out);

        std::fs::rename(&tmp_path, &self.path)?;
        // Make the rename itself durable: without an fsync of the directory a
        // crash can lose the directory entry even though the new file's
        // contents were fully synced.
        sync_parent_dir(&self.path)?;

        *file = {
            let new_file = OpenOptions::new().read(true).write(true).open(&self.path)?;
            // Re-acquire the exclusive lock on the new inode before swapping.
            #[cfg(not(target_arch = "wasm32"))]
            new_file.try_lock_exclusive().map_err(|e| {
                Error::ResourceLimit(format!(
                    "cannot re-lock {} after compaction: {}",
                    self.path.display(),
                    e
                ))
            })?;
            new_file
        };
        *docs_guard = new_docs;
        self.total_size.store(total_size, Ordering::SeqCst);
        self.next_offset.store(header.data_end, Ordering::SeqCst);
        self.seq.store(header.seq, Ordering::SeqCst);

        drop(file);
        drop(docs_guard);

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

        // Lock order is documents -> file everywhere. Holding the map lock
        // across offset allocation AND the file write serialises us against
        // compact(): an offset handed out here always belongs to the inode
        // currently at `self.path`, because compaction cannot replace the
        // file until we release the map lock.
        let mut docs = self.documents.write();
        let offset = self.next_offset.fetch_add(size as u64 + 8, Ordering::SeqCst);
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset))?;
            file.write_u32::<LittleEndian>(size)?;
            file.write_u32::<LittleEndian>(checksum)?;
            file.write_all(data)?;
        }
        docs.insert(id, DocumentMeta {
            offset: offset + 8, // Skip size/checksum header
            size,
            checksum,
        });

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

                    if self.config.verify_checksums_on_read {
                        let actual = compute_checksum(&data);
                        if actual != meta.checksum {
                            return Err(Error::Corrupted("Document checksum mismatch".into()));
                        }
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

        if self.config.verify_checksums_on_read {
            let actual = compute_checksum(&data);
            if actual != meta.checksum {
                return Err(Error::Corrupted("Document checksum mismatch".into()));
            }
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
        // Serialise commits: two concurrent flushes must not load the same
        // `seq` and both write header slot `(seq+1) % HEADER_SLOTS`, the
        // second clobbering the first.
        let _flush_guard = self.flush_lock.lock();

        let trie_data = bincode::serialize(trie)?;
        let trie_crc = compute_checksum(&trie_data);

        // Serialise the document index into one blob so it gets a single CRC.
        // Sorted by UUID: deterministic bytes for identical logical state.
        let index = {
            let docs = self.documents.read();
            serialize_index(&docs)?
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

/// fsync the directory containing `path` so that a rename into it (used by
/// `compact`) is durable across a crash. No-op off Unix.
#[cfg(all(feature = "persistence", unix))]
fn sync_parent_dir(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// fsync the directory containing `path` so that a rename into it (used by
/// `compact`) is durable across a crash. No-op off Unix.
#[cfg(all(feature = "persistence", not(unix)))]
fn sync_parent_dir(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

/// Serialise the document index into one blob with a single CRC.
///
/// Entries are sorted by UUID so identical logical state always produces
/// identical bytes: `HashMap` iteration order is randomised per process, and
/// without the sort two runs of the same workload would write different
/// (though equally valid) index blobs and header CRCs.
#[cfg(feature = "persistence")]
fn serialize_index(docs: &HashMap<Uuid, DocumentMeta>) -> Result<Vec<u8>> {
    let mut entries: Vec<(&Uuid, &DocumentMeta)> = docs.iter().collect();
    entries.sort_unstable_by_key(|(id, _)| *id);

    let mut buf = Vec::with_capacity(8 + entries.len() * 32);
    buf.write_u64::<LittleEndian>(entries.len() as u64)?;
    for (id, meta) in entries {
        buf.write_all(id.as_bytes())?;
        buf.write_u64::<LittleEndian>(meta.offset)?;
        buf.write_u32::<LittleEndian>(meta.size)?;
        buf.write_u32::<LittleEndian>(meta.checksum)?;
    }
    Ok(buf)
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

    /// Identical logical state must serialize to identical index bytes:
    /// `HashMap` iteration order is randomised per process, so the index
    /// is written sorted by UUID (see `serialize_index`).
    #[cfg(feature = "persistence")]
    #[test]
    fn index_serialization_is_order_independent() {
        let mut a = HashMap::new();
        let mut b = HashMap::new();
        let ids: Vec<Uuid> = (0..50).map(|_| Uuid::new_v4()).collect();

        for (i, id) in ids.iter().enumerate() {
            a.insert(
                *id,
                DocumentMeta {
                    offset: (i * 40) as u64,
                    size: 32,
                    checksum: 0xDEAD_BEEF,
                },
            );
        }
        for (i, id) in ids.iter().enumerate().rev() {
            b.insert(
                *id,
                DocumentMeta {
                    offset: (i * 40) as u64,
                    size: 32,
                    checksum: 0xDEAD_BEEF,
                },
            );
        }

        assert_eq!(serialize_index(&a).unwrap(), serialize_index(&b).unwrap());
    }

    /// A `write()` that reserves an offset just before `compact()` swaps the
    /// file must never land at that stale offset inside the new inode: every
    /// document live at compact time must still read back checksum-clean.
    #[cfg(feature = "persistence")]
    #[test]
    fn compact_survives_concurrent_writers() {
        use std::sync::Arc;
        use std::thread;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("race.db");
        let (backend, mut trie) = FileBackend::open(&path, &Config::default()).unwrap();
        let backend = Arc::new(backend);

        // Seed 60 docs, no flush: old next_offset sits right after the last
        // document, which is exactly where the compacted file puts its
        // trie/index blobs — the stale-offset clobber window.
        let mut live = Vec::new();
        for i in 0..60u32 {
            let id = backend.write(&vec![i as u8; 512]).unwrap();
            trie = trie.insert(format!("{i}.d").as_bytes(), id);
            live.push((i, id));
        }

        // Large racer docs: keeps the writer mid-loop while compact() runs,
        // so at least one write has reserved its offset pre-compact and
        // blocks on the file lock until after the swap.
        let b2 = Arc::clone(&backend);
        let writer = thread::spawn(move || {
            for j in 0..30u32 {
                b2.write(&vec![j as u8; 64 * 1024]).unwrap();
            }
        });

        backend.compact(&trie).unwrap();
        writer.join().unwrap();
        drop(backend);

        let (backend, trie2) = FileBackend::open(&path, &Config::default()).unwrap();
        for (i, id) in &live {
            assert_eq!(
                trie2.get(format!("{i}.d").as_bytes()),
                Some(*id),
                "key {i} lost across racing compaction"
            );
            let data = backend.read(*id).unwrap();
            assert!(
                data.iter().all(|&b| b == *i as u8),
                "doc {i} clobbered by a racing write"
            );
        }
    }

    /// A `delete()` issued while `compact()` is mid-copy must be honoured:
    /// the document must not reappear after reopening.
    #[cfg(feature = "persistence")]
    #[test]
    fn compact_honours_deletes_that_land_mid_compaction() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("delrace.db");
        let (backend, mut trie) = FileBackend::open(&path, &Config::default()).unwrap();
        let backend = Arc::new(backend);

        // Enough data that the copy phase takes long enough for a racing
        // delete to land inside it.
        let mut doomed = Vec::new();
        for i in 0..2000u32 {
            let id = backend.write(&vec![i as u8; 4096]).unwrap();
            trie = trie.insert(format!("{i}.x").as_bytes(), id);
            if i < 50 {
                doomed.push((i, id));
            }
        }
        backend.flush(&trie).unwrap();

        let doomed_ids: Vec<Uuid> = doomed.iter().map(|(_, id)| *id).collect();
        let b2 = Arc::clone(&backend);
        let deleter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            for id in &doomed_ids {
                // NotFound is fine: compact() also reclaims trie-unreferenced
                // documents, and the trie being compacted excludes these —
                // a tie between delete and GC honours the contract either way.
                let _ = b2.delete(*id);
            }
        });

        // Compact with a trie that already excludes the doomed keys.
        let mut trie2 = trie;
        for (i, _) in &doomed {
            trie2 = trie2.remove(format!("{i}.x").as_bytes()).unwrap();
        }
        backend.compact(&trie2).unwrap();
        deleter.join().unwrap();
        backend.flush(&trie2).unwrap();
        drop(backend);

        let (backend, trie3) = FileBackend::open(&path, &Config::default()).unwrap();
        for (i, id) in &doomed {
            assert_eq!(trie3.get(format!("{i}.x").as_bytes()), None);
            assert!(
                backend.read(*id).is_err(),
                "deleted document {i} resurrected by compaction"
            );
        }
        // Survivors intact.
        for i in 50..60u32 {
            let id = trie3.get(format!("{i}.x").as_bytes()).unwrap();
            assert!(backend.read(id).unwrap().iter().all(|&b| b == i as u8));
        }
    }

    /// Two handles on the same path keep independent `next_offset`s and
    /// would interleave appends, corrupting the store: a second `open`
    /// while the first is alive must fail, and succeed again after drop.
    #[cfg(feature = "persistence")]
    #[test]
    fn second_open_of_same_path_fails() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("locked.db");

        let (backend, _trie) = FileBackend::open(&path, &Config::default()).unwrap();

        match FileBackend::open(&path, &Config::default()) {
            Err(Error::ResourceLimit(_)) => {}
            Err(e) => panic!("expected ResourceLimit, got: {e}"),
            Ok(_) => panic!("double open must fail while first handle is alive"),
        }

        // First handle still usable after the rejected attempt.
        let id = backend.write(b"still alive").unwrap();
        assert_eq!(backend.read(id).unwrap(), b"still alive");

        drop(backend);
        let (_b2, _t2) = FileBackend::open(&path, &Config::default()).unwrap();
    }

    /// Concurrent flushes must be serialised into monotonic commits: the
    /// file must load cleanly afterwards no matter how they interleaved.
    #[cfg(feature = "persistence")]
    #[test]
    fn concurrent_flushes_commit_cleanly() {
        use std::sync::Arc;
        use std::thread;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("flushrace.db");
        let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();
        let backend = Arc::new(backend);

        let mut handles = vec![];
        for _ in 0..4 {
            let b = Arc::clone(&backend);
            let t = trie.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    b.flush(&t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        drop(backend);

        // Must load cleanly; exactly one valid newest commit.
        let (_b, _t) = FileBackend::open(&path, &Config::default()).unwrap();
    }

    /// Checksum verification on read defaults ON and can be disabled.
    #[cfg(feature = "persistence")]
    #[test]
    fn checksum_on_read_defaults_on_and_can_be_disabled() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("crc.db");

        let mut cfg = Config::default();
        assert!(cfg.verify_checksums_on_read, "default must be ON");
        cfg.verify_checksums_on_read = false;

        let (backend, _trie) = FileBackend::open(&path, &cfg).unwrap();
        let id = backend.write(b"data").unwrap();
        assert_eq!(backend.read(id).unwrap(), b"data");
    }

    /// Documents not referenced by the committed trie (superseded updates
    /// whose cleanup failed) are reclaimed by compaction.
    #[cfg(feature = "persistence")]
    #[test]
    fn compact_reclaims_orphaned_documents() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("orphan.db");

        let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();

        // Orphan: written, never referenced by the trie.
        let orphan = backend.write(b"orphan payload").unwrap();
        // Live: written and referenced.
        let live = backend.write(b"live payload").unwrap();
        let trie = trie.insert(b"live", live);
        backend.flush(&trie).unwrap();
        assert_eq!(backend.read(orphan).unwrap(), b"orphan payload");

        backend.compact(&trie).unwrap();

        assert!(
            backend.read(orphan).is_err(),
            "orphaned document survived compaction"
        );
        assert_eq!(backend.read(live).unwrap(), b"live payload");
    }
}
