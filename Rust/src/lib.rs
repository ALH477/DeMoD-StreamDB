//! # StreamDB
//!
//! A lightweight, thread-safe embedded key-value database using a reverse trie
//! for efficient suffix-based searches.
//!
//! ## Features
//!
//! - **Thread-safe**: All operations use interior mutability with proper locking
//! - **Suffix search**: O(m + k) lookup for keys ending with a given suffix
//! - **Binary keys**: Supports arbitrary byte sequences
//! - **Persistence**: Optional file-based storage with crash recovery
//! - **Async support**: Optional async API via Tokio
//! - **FFI bindings**: C-compatible interface for cross-language use
//!
//! ## Quick Start
//!
//! ```rust
//! use streamdb::{StreamDb, Config, Result};
//!
//! fn main() -> Result<()> {
//!     // Create an in-memory database
//!     let db = StreamDb::open_memory()?;
//!     
//!     // Insert data
//!     let id = db.insert(b"user:alice", b"Alice Smith")?;
//!     
//!     // Retrieve data
//!     let value = db.get(b"user:alice")?;
//!     assert_eq!(value, Some(b"Alice Smith".to_vec()));
//!     
//!     // Suffix search - find all keys ending with "alice"
//!     let results = db.suffix_search(b"alice")?;
//!     assert!(results.iter().any(|(k, _)| k == b"user:alice"));
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Persistence
//!
//! ```rust,no_run
//! use streamdb::{StreamDb, Config, Result};
//!
//! fn main() -> Result<()> {
//!     // Open with file persistence
//!     let config = Config::default();
//!     let db = StreamDb::open("mydb.dat", config)?;
//!     
//!     db.insert(b"key", b"value")?;
//!     db.flush()?;  // Ensure data is persisted
//!     
//!     Ok(())
//! }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod error;
pub mod trie;
pub mod storage;
#[cfg(feature = "ffi")]
#[cfg_attr(docsrs, doc(cfg(feature = "ffi")))]
pub mod ffi;

pub use error::{Error, Result};
pub use trie::Trie;
pub use storage::{Backend, MemoryBackend, Stats};

#[cfg(feature = "persistence")]
pub use storage::FileBackend;

use parking_lot::{Mutex, RwLock};
use lru::LruCache;
use uuid::Uuid;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use log::{debug, info, warn};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum key length in bytes
pub const MAX_KEY_LEN: usize = 1024;

/// Maximum value size (256 MB)
pub const MAX_VALUE_SIZE: usize = 256 * 1024 * 1024;

/// Configuration for StreamDb
#[derive(Debug, Clone)]
pub struct Config {
    /// Size of pages for file storage (default: 4096)
    pub page_size: usize,
    
    /// Maximum number of entries in the path cache (default: 10000)
    pub cache_size: usize,
    
    /// Auto-flush interval in milliseconds (0 to disable, default: 5000)
    pub flush_interval_ms: u64,
    
    /// Enable memory-mapped I/O when available (default: true)
    pub use_mmap: bool,
    
    /// Enable compression for values (default: false)
    #[cfg(feature = "compression")]
    pub use_compression: bool,
    
    /// Encryption key for data at rest (32 bytes for AES-256)
    #[cfg(feature = "encryption")]
    pub encryption_key: Option<[u8; 32]>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            page_size: 4096,
            cache_size: 10000,
            flush_interval_ms: 5000,
            use_mmap: true,
            #[cfg(feature = "compression")]
            use_compression: false,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        }
    }
}

/// Result entry from a search operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResult {
    /// The full key
    pub key: Vec<u8>,
    /// The document ID
    pub id: Uuid,
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
}

impl CacheStats {
    /// Calculate hit ratio (0.0 to 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// The main StreamDB database handle
///
/// This is the primary interface for interacting with StreamDB.
/// All methods are thread-safe and can be called from multiple threads.
pub struct StreamDb {
    backend: Arc<dyn Backend>,
    trie: Arc<RwLock<Trie>>,
    cache: Mutex<LruCache<Vec<u8>, Uuid>>,
    cache_stats: CacheStats,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    dirty: AtomicBool,
    config: Config,
}

impl StreamDb {
    /// Open a database with file persistence
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file
    /// * `config` - Configuration options
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use streamdb::{StreamDb, Config};
    ///
    /// let db = StreamDb::open("mydb.dat", Config::default()).unwrap();
    /// ```
    #[cfg(feature = "persistence")]
    #[cfg_attr(docsrs, doc(cfg(feature = "persistence")))]
    pub fn open<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        info!("Opening StreamDb at {:?}", path.as_ref());
        
        let (backend, trie) = FileBackend::open(path.as_ref(), &config)?;
        
        Ok(Self {
            backend: Arc::new(backend),
            trie: Arc::new(RwLock::new(trie)),
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(config.cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap())
            )),
            cache_stats: CacheStats::default(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            config,
        })
    }
    
    /// Create an in-memory database (no persistence)
    ///
    /// # Example
    ///
    /// ```rust
    /// use streamdb::StreamDb;
    ///
    /// let db = StreamDb::open_memory().unwrap();
    /// ```
    pub fn open_memory() -> Result<Self> {
        Self::open_memory_with_config(Config::default())
    }
    
    /// Create an in-memory database with custom configuration
    pub fn open_memory_with_config(config: Config) -> Result<Self> {
        info!("Opening in-memory StreamDb");
        
        Ok(Self {
            backend: Arc::new(MemoryBackend::new()),
            trie: Arc::new(RwLock::new(Trie::new())),
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(config.cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap())
            )),
            cache_stats: CacheStats::default(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            config,
        })
    }
    
    /// Insert or update a key-value pair
    ///
    /// Returns the document ID for the inserted value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key (max 1024 bytes)
    /// * `value` - The value (max 256 MB)
    ///
    /// # Example
    ///
    /// ```rust
    /// use streamdb::StreamDb;
    ///
    /// let db = StreamDb::open_memory().unwrap();
    /// let id = db.insert(b"mykey", b"myvalue").unwrap();
    /// ```
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Uuid> {
        self.validate_key(key)?;
        self.validate_value(value)?;
        
        debug!("Inserting key: {:?} ({} bytes value)", key, value.len());
        
        // Write value to backend
        let id = self.backend.write(value)?;
        
        // Update trie
        {
            let mut trie = self.trie.write();
            *trie = trie.insert(key, id);
        }
        
        // Update cache
        {
            let mut cache = self.cache.lock();
            cache.put(key.to_vec(), id);
        }
        
        self.dirty.store(true, Ordering::Release);
        
        Ok(id)
    }
    
    /// Get a value by key
    ///
    /// Returns `None` if the key doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use streamdb::StreamDb;
    ///
    /// let db = StreamDb::open_memory().unwrap();
    /// db.insert(b"key", b"value").unwrap();
    /// 
    /// let value = db.get(b"key").unwrap();
    /// assert_eq!(value, Some(b"value".to_vec()));
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;
        
        debug!("Getting key: {:?}", key);
        
        // Check cache first
        let id = {
            let mut cache = self.cache.lock();
            if let Some(&id) = cache.get(&key.to_vec()) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(id)
            } else {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        };
        
        // If not in cache, check trie
        let id = match id {
            Some(id) => Some(id),
            None => {
                let trie = self.trie.read();
                let found_id = trie.get(key);
                
                // Update cache if found
                if let Some(id) = found_id {
                    let mut cache = self.cache.lock();
                    cache.put(key.to_vec(), id);
                }
                
                found_id
            }
        };
        
        // Read from backend
        match id {
            Some(id) => {
                let value = self.backend.read(id)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
    
    /// Check if a key exists
    ///
    /// More efficient than `get()` as it doesn't read the value.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        self.validate_key(key)?;
        
        // Check cache first
        {
            let mut cache = self.cache.lock();
            if cache.get(&key.to_vec()).is_some() {
                return Ok(true);
            }
        }
        
        // Check trie
        let trie = self.trie.read();
        Ok(trie.get(key).is_some())
    }
    
    /// Delete a key-value pair
    ///
    /// Returns `true` if the key existed and was deleted.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        self.validate_key(key)?;
        
        debug!("Deleting key: {:?}", key);
        
        // Get ID from trie
        let id = {
            let trie = self.trie.read();
            trie.get(key)
        };
        
        let Some(id) = id else {
            return Ok(false);
        };
        
        // Remove from trie
        {
            let mut trie = self.trie.write();
            if let Some(new_trie) = trie.remove(key) {
                *trie = new_trie;
            }
        }
        
        // Remove from cache
        {
            let mut cache = self.cache.lock();
            cache.pop(&key.to_vec());
        }
        
        // Delete from backend
        self.backend.delete(id)?;
        
        self.dirty.store(true, Ordering::Release);
        
        Ok(true)
    }
    
    /// Search for all keys ending with the given suffix
    ///
    /// Returns a list of matching keys and their document IDs.
    ///
    /// # Example
    ///
    /// ```rust
    /// use streamdb::StreamDb;
    ///
    /// let db = StreamDb::open_memory().unwrap();
    /// db.insert(b"user:alice", b"Alice").unwrap();
    /// db.insert(b"user:bob", b"Bob").unwrap();
    /// db.insert(b"admin:alice", b"Alice Admin").unwrap();
    ///
    /// // Find all keys ending with "alice"
    /// let results = db.suffix_search(b"alice").unwrap();
    /// assert_eq!(results.len(), 2);
    /// ```
    pub fn suffix_search(&self, suffix: &[u8]) -> Result<Vec<SearchResult>> {
        if suffix.is_empty() {
            return Err(Error::InvalidInput("Suffix cannot be empty".into()));
        }
        if suffix.len() > MAX_KEY_LEN {
            return Err(Error::InvalidInput(format!(
                "Suffix too long: {} bytes (max {})",
                suffix.len(),
                MAX_KEY_LEN
            )));
        }
        
        debug!("Suffix search: {:?}", suffix);
        
        let trie = self.trie.read();
        let results = trie.suffix_search(suffix);
        
        Ok(results.into_iter()
            .map(|(key, id)| SearchResult { key, id })
            .collect())
    }
    
    /// Iterate over all key-value pairs
    ///
    /// The callback receives each key and document ID. Return `true` to continue,
    /// `false` to stop iteration early.
    pub fn for_each<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], Uuid) -> bool,
    {
        let trie = self.trie.read();
        trie.for_each(&mut callback);
        Ok(())
    }
    
    /// Flush all pending writes to disk
    ///
    /// This is automatically called on drop, but can be called manually
    /// to ensure durability at specific points.
    pub fn flush(&self) -> Result<()> {
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        
        info!("Flushing database");
        
        // Serialize and write trie
        let trie = self.trie.read();
        self.backend.flush(&trie)?;
        
        self.dirty.store(false, Ordering::Release);
        
        Ok(())
    }
    
    /// Get database statistics
    pub fn stats(&self) -> Result<Stats> {
        let trie = self.trie.read();
        let backend_stats = self.backend.stats()?;
        
        Ok(Stats {
            key_count: trie.len(),
            total_size: backend_stats.total_size,
            cache_stats: CacheStats {
                hits: self.cache_hits.load(Ordering::Relaxed),
                misses: self.cache_misses.load(Ordering::Relaxed),
            },
            is_dirty: self.dirty.load(Ordering::Relaxed),
        })
    }
    
    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            hits: self.cache_hits.load(Ordering::Relaxed),
            misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
    
    /// Clear the path cache
    pub fn clear_cache(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
    }
    
    /// Get the number of keys in the database
    pub fn len(&self) -> usize {
        let trie = self.trie.read();
        trie.len()
    }
    
    /// Check if the database is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    // Validation helpers
    
    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidInput("Key cannot be empty".into()));
        }
        if key.len() > MAX_KEY_LEN {
            return Err(Error::InvalidInput(format!(
                "Key too long: {} bytes (max {})",
                key.len(),
                MAX_KEY_LEN
            )));
        }
        Ok(())
    }
    
    fn validate_value(&self, value: &[u8]) -> Result<()> {
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::InvalidInput(format!(
                "Value too large: {} bytes (max {})",
                value.len(),
                MAX_VALUE_SIZE
            )));
        }
        Ok(())
    }
}

impl Drop for StreamDb {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            warn!("Failed to flush on drop: {}", e);
        }
    }
}

// StreamDb is Send + Sync because all interior mutability is properly synchronized
unsafe impl Send for StreamDb {}
unsafe impl Sync for StreamDb {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_operations() {
        let db = StreamDb::open_memory().unwrap();
        
        // Insert
        let id = db.insert(b"key1", b"value1").unwrap();
        assert!(!id.is_nil());
        
        // Get
        let value = db.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        // Exists
        assert!(db.exists(b"key1").unwrap());
        assert!(!db.exists(b"nonexistent").unwrap());
        
        // Delete
        assert!(db.delete(b"key1").unwrap());
        assert!(!db.delete(b"key1").unwrap()); // Already deleted
        
        // Verify deletion
        assert_eq!(db.get(b"key1").unwrap(), None);
    }
    
    #[test]
    fn test_suffix_search() {
        let db = StreamDb::open_memory().unwrap();
        
        db.insert(b"user:alice", b"Alice").unwrap();
        db.insert(b"user:bob", b"Bob").unwrap();
        db.insert(b"admin:alice", b"Alice Admin").unwrap();
        db.insert(b"guest:charlie", b"Charlie").unwrap();
        
        // Search for "alice"
        let results = db.suffix_search(b"alice").unwrap();
        assert_eq!(results.len(), 2);
        
        let keys: Vec<_> = results.iter().map(|r| r.key.as_slice()).collect();
        assert!(keys.contains(&b"user:alice".as_slice()));
        assert!(keys.contains(&b"admin:alice".as_slice()));
        
        // Search for "bob"
        let results = db.suffix_search(b"bob").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"user:bob");
    }
    
    #[test]
    fn test_binary_keys() {
        let db = StreamDb::open_memory().unwrap();
        
        let key = vec![0x00, 0x01, 0xFF, 0xFE, 0x00];
        let value = b"binary value";
        
        db.insert(&key, value).unwrap();
        
        let retrieved = db.get(&key).unwrap();
        assert_eq!(retrieved, Some(value.to_vec()));
    }
    
    #[test]
    fn test_update() {
        let db = StreamDb::open_memory().unwrap();
        
        db.insert(b"key", b"value1").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value1".to_vec()));
        
        db.insert(b"key", b"value2").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value2".to_vec()));
    }
    
    #[test]
    fn test_stats() {
        let db = StreamDb::open_memory().unwrap();
        
        db.insert(b"key1", b"value1").unwrap();
        db.insert(b"key2", b"value2").unwrap();
        
        let stats = db.stats().unwrap();
        assert_eq!(stats.key_count, 2);
    }
    
    #[test]
    fn test_key_validation() {
        let db = StreamDb::open_memory().unwrap();
        
        // Empty key
        assert!(db.insert(b"", b"value").is_err());
        
        // Key too long
        let long_key = vec![0u8; MAX_KEY_LEN + 1];
        assert!(db.insert(&long_key, b"value").is_err());
        
        // Max length key should work
        let max_key = vec![0u8; MAX_KEY_LEN];
        assert!(db.insert(&max_key, b"value").is_ok());
    }
    
    #[test]
    fn test_concurrent_access() {
        use std::thread;
        use std::sync::Arc;
        
        let db = Arc::new(StreamDb::open_memory().unwrap());
        let mut handles = vec![];
        
        // Spawn writers
        for i in 0..10 {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key:{}:{}", i, j);
                    let value = format!("value:{}:{}", i, j);
                    db.insert(key.as_bytes(), value.as_bytes()).unwrap();
                }
            }));
        }
        
        // Wait for all writers
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify count
        assert_eq!(db.len(), 1000);
    }
}
