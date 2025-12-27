//! Foreign Function Interface (FFI) for StreamDB.
//!
//! This module provides a C-compatible interface for StreamDB, enabling
//! integration with non-Rust systems (C, C++, Python, etc.).
//!
//! # Safety
//!
//! All functions perform null checks and catch panics to prevent undefined
//! behavior when called from C code. Errors are returned via status codes.
//!
//! # Memory Management
//!
//! - Buffers returned by StreamDB must be freed with `streamdb_free_buffer`
//! - String arrays must be freed with `streamdb_free_string_array`
//! - The database handle must be closed with `streamdb_close`
//!
//! # Example (C)
//!
//! ```c
//! StreamDbHandle* db = NULL;
//! if (streamdb_open("test.db", &db) != STREAMDB_OK) {
//!     // handle error
//! }
//!
//! // Write data
//! const char* data = "hello";
//! StreamDbBuffer buffer = {0};
//! if (streamdb_insert(db, "key", 3, data, 5, &buffer) != STREAMDB_OK) {
//!     // handle error
//! }
//!
//! // Read data
//! StreamDbBuffer result = {0};
//! if (streamdb_get(db, "key", 3, &result) == STREAMDB_OK) {
//!     // use result.data, result.len
//!     streamdb_free_buffer(&result);
//! }
//!
//! streamdb_close(db);
//! ```

use crate::{StreamDb, Config, Error, Result, SearchResult};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// Types
// ============================================================================

/// Opaque handle to a StreamDB instance.
#[repr(C)]
pub struct StreamDbHandle {
    inner: Arc<StreamDb>,
}

/// Buffer for returning binary data to C.
#[repr(C)]
pub struct StreamDbBuffer {
    /// Pointer to data (owned by StreamDB, must be freed)
    pub data: *mut u8,
    /// Length of data in bytes
    pub len: usize,
    /// Capacity (for internal use)
    capacity: usize,
}

/// UUID type for C (128-bit).
#[repr(C)]
#[derive(Copy, Clone)]
pub struct StreamDbUuid {
    /// UUID bytes in big-endian order
    pub bytes: [u8; 16],
}

/// Search result for C.
#[repr(C)]
pub struct StreamDbSearchResult {
    /// Key bytes
    pub key: *mut u8,
    /// Key length
    pub key_len: usize,
    /// Document UUID
    pub id: StreamDbUuid,
}

/// Array of search results.
#[repr(C)]
pub struct StreamDbSearchResults {
    /// Array of results
    pub results: *mut StreamDbSearchResult,
    /// Number of results
    pub count: usize,
}

/// Database statistics.
#[repr(C)]
pub struct StreamDbStats {
    /// Number of keys
    pub key_count: usize,
    /// Total value size in bytes
    pub total_size: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

// ============================================================================
// Status Codes
// ============================================================================

/// Operation succeeded
pub const STREAMDB_OK: c_int = 0;
/// I/O error
pub const STREAMDB_ERR_IO: c_int = -1;
/// Key/document not found
pub const STREAMDB_ERR_NOT_FOUND: c_int = -2;
/// Invalid input (null pointer, invalid UTF-8, etc.)
pub const STREAMDB_ERR_INVALID: c_int = -3;
/// Internal error (panic caught)
pub const STREAMDB_ERR_INTERNAL: c_int = -4;
/// Data corruption detected
pub const STREAMDB_ERR_CORRUPTED: c_int = -5;

// ============================================================================
// Helper Functions
// ============================================================================

impl StreamDbBuffer {
    fn from_vec(vec: Vec<u8>) -> Self {
        let mut vec = vec.into_boxed_slice();
        let data = vec.as_mut_ptr();
        let len = vec.len();
        std::mem::forget(vec);
        
        Self {
            data,
            len,
            capacity: len,
        }
    }
    
    fn null() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }
}

impl StreamDbUuid {
    fn from_uuid(uuid: Uuid) -> Self {
        Self {
            bytes: *uuid.as_bytes(),
        }
    }
    
    fn to_uuid(self) -> Uuid {
        Uuid::from_bytes(self.bytes)
    }
}

fn error_to_code(err: &Error) -> c_int {
    match err {
        Error::Io(_) => STREAMDB_ERR_IO,
        Error::NotFound(_) => STREAMDB_ERR_NOT_FOUND,
        Error::InvalidInput(_) => STREAMDB_ERR_INVALID,
        Error::Corrupted(_) => STREAMDB_ERR_CORRUPTED,
        _ => STREAMDB_ERR_INTERNAL,
    }
}

/// Wrapper to catch panics and convert to error code.
fn catch_panic<F, T>(f: F) -> std::result::Result<T, c_int>
where
    F: FnOnce() -> Result<T> + panic::UnwindSafe,
{
    match panic::catch_unwind(f) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(error_to_code(&e)),
        Err(_) => Err(STREAMDB_ERR_INTERNAL),
    }
}

// ============================================================================
// Database Lifecycle
// ============================================================================

/// Open or create a database file.
///
/// # Safety
///
/// - `path` must be a valid null-terminated UTF-8 string
/// - `out_handle` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn streamdb_open(
    path: *const c_char,
    out_handle: *mut *mut StreamDbHandle,
) -> c_int {
    if path.is_null() || out_handle.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let path_str = unsafe { CStr::from_ptr(path) }
            .to_str()
            .map_err(|_| Error::InvalidInput("Invalid UTF-8 in path".into()))?;
        
        #[cfg(feature = "persistence")]
        let db = StreamDb::open(path_str, Config::default())?;
        
        #[cfg(not(feature = "persistence"))]
        let db = StreamDb::open_memory()?;
        
        Ok(db)
    }));
    
    match result {
        Ok(db) => {
            let handle = Box::new(StreamDbHandle {
                inner: Arc::new(db),
            });
            unsafe { *out_handle = Box::into_raw(handle) };
            STREAMDB_OK
        }
        Err(code) => code,
    }
}

/// Open an in-memory database (no persistence).
///
/// # Safety
///
/// - `out_handle` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn streamdb_open_memory(
    out_handle: *mut *mut StreamDbHandle,
) -> c_int {
    if out_handle.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        StreamDb::open_memory()
    }));
    
    match result {
        Ok(db) => {
            let handle = Box::new(StreamDbHandle {
                inner: Arc::new(db),
            });
            unsafe { *out_handle = Box::into_raw(handle) };
            STREAMDB_OK
        }
        Err(code) => code,
    }
}

/// Close a database and free resources.
///
/// # Safety
///
/// - `handle` must be a valid handle from `streamdb_open` or `streamdb_open_memory`
/// - `handle` must not be used after this call
#[no_mangle]
pub unsafe extern "C" fn streamdb_close(handle: *mut StreamDbHandle) {
    if !handle.is_null() {
        let _ = unsafe { Box::from_raw(handle) };
    }
}

// ============================================================================
// Core Operations
// ============================================================================

/// Insert or update a key-value pair.
///
/// Returns the document UUID in `out_id` if provided.
///
/// # Safety
///
/// - `handle` must be a valid database handle
/// - `key` must point to at least `key_len` bytes
/// - `value` must point to at least `value_len` bytes
/// - `out_id` may be null if the ID is not needed
#[no_mangle]
pub unsafe extern "C" fn streamdb_insert(
    handle: *mut StreamDbHandle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    out_id: *mut StreamDbUuid,
) -> c_int {
    if handle.is_null() || key.is_null() || (value_len > 0 && value.is_null()) {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        let value_slice = if value_len > 0 {
            unsafe { slice::from_raw_parts(value, value_len) }
        } else {
            &[]
        };
        
        db.insert(key_slice, value_slice)
    }));
    
    match result {
        Ok(id) => {
            if !out_id.is_null() {
                unsafe { *out_id = StreamDbUuid::from_uuid(id) };
            }
            STREAMDB_OK
        }
        Err(code) => code,
    }
}

/// Get a value by key.
///
/// # Safety
///
/// - `handle` must be a valid database handle
/// - `key` must point to at least `key_len` bytes
/// - `out_buffer` must be a valid pointer
/// - The returned buffer must be freed with `streamdb_free_buffer`
#[no_mangle]
pub unsafe extern "C" fn streamdb_get(
    handle: *mut StreamDbHandle,
    key: *const u8,
    key_len: usize,
    out_buffer: *mut StreamDbBuffer,
) -> c_int {
    if handle.is_null() || key.is_null() || out_buffer.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        
        db.get(key_slice)
    }));
    
    match result {
        Ok(Some(value)) => {
            unsafe { *out_buffer = StreamDbBuffer::from_vec(value) };
            STREAMDB_OK
        }
        Ok(None) => {
            unsafe { *out_buffer = StreamDbBuffer::null() };
            STREAMDB_ERR_NOT_FOUND
        }
        Err(code) => {
            unsafe { *out_buffer = StreamDbBuffer::null() };
            code
        }
    }
}

/// Check if a key exists.
///
/// # Safety
///
/// - `handle` must be a valid database handle
/// - `key` must point to at least `key_len` bytes
/// - `out_exists` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn streamdb_exists(
    handle: *mut StreamDbHandle,
    key: *const u8,
    key_len: usize,
    out_exists: *mut c_int,
) -> c_int {
    if handle.is_null() || key.is_null() || out_exists.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        
        db.exists(key_slice)
    }));
    
    match result {
        Ok(exists) => {
            unsafe { *out_exists = if exists { 1 } else { 0 } };
            STREAMDB_OK
        }
        Err(code) => code,
    }
}

/// Delete a key-value pair.
///
/// # Safety
///
/// - `handle` must be a valid database handle
/// - `key` must point to at least `key_len` bytes
#[no_mangle]
pub unsafe extern "C" fn streamdb_delete(
    handle: *mut StreamDbHandle,
    key: *const u8,
    key_len: usize,
) -> c_int {
    if handle.is_null() || key.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        
        db.delete(key_slice)
    }));
    
    match result {
        Ok(true) => STREAMDB_OK,
        Ok(false) => STREAMDB_ERR_NOT_FOUND,
        Err(code) => code,
    }
}

// ============================================================================
// Search Operations
// ============================================================================

/// Search for keys ending with the given suffix.
///
/// # Safety
///
/// - `handle` must be a valid database handle
/// - `suffix` must point to at least `suffix_len` bytes
/// - `out_results` must be a valid pointer
/// - The returned results must be freed with `streamdb_free_search_results`
#[no_mangle]
pub unsafe extern "C" fn streamdb_suffix_search(
    handle: *mut StreamDbHandle,
    suffix: *const u8,
    suffix_len: usize,
    out_results: *mut StreamDbSearchResults,
) -> c_int {
    if handle.is_null() || suffix.is_null() || out_results.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        let suffix_slice = unsafe { slice::from_raw_parts(suffix, suffix_len) };
        
        db.suffix_search(suffix_slice)
    }));
    
    match result {
        Ok(results) => {
            let count = results.len();
            
            if count == 0 {
                unsafe {
                    *out_results = StreamDbSearchResults {
                        results: ptr::null_mut(),
                        count: 0,
                    };
                }
                return STREAMDB_OK;
            }
            
            // Allocate results array
            let mut c_results: Vec<StreamDbSearchResult> = results
                .into_iter()
                .map(|r| {
                    let mut key = r.key.into_boxed_slice();
                    let key_ptr = key.as_mut_ptr();
                    let key_len = key.len();
                    std::mem::forget(key);
                    
                    StreamDbSearchResult {
                        key: key_ptr,
                        key_len,
                        id: StreamDbUuid::from_uuid(r.id),
                    }
                })
                .collect();
            
            let results_ptr = c_results.as_mut_ptr();
            std::mem::forget(c_results);
            
            unsafe {
                *out_results = StreamDbSearchResults {
                    results: results_ptr,
                    count,
                };
            }
            
            STREAMDB_OK
        }
        Err(code) => code,
    }
}

/// Free search results.
///
/// # Safety
///
/// - `results` must be a valid pointer from `streamdb_suffix_search`
#[no_mangle]
pub unsafe extern "C" fn streamdb_free_search_results(results: *mut StreamDbSearchResults) {
    if results.is_null() {
        return;
    }
    
    let results = unsafe { &*results };
    
    if !results.results.is_null() && results.count > 0 {
        let slice = unsafe {
            slice::from_raw_parts_mut(results.results, results.count)
        };
        
        // Free each key
        for result in slice.iter() {
            if !result.key.is_null() {
                let _ = unsafe {
                    Vec::from_raw_parts(result.key, result.key_len, result.key_len)
                };
            }
        }
        
        // Free the array itself
        let _ = unsafe {
            Vec::from_raw_parts(results.results, results.count, results.count)
        };
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Flush pending writes to disk.
///
/// # Safety
///
/// - `handle` must be a valid database handle
#[no_mangle]
pub unsafe extern "C" fn streamdb_flush(handle: *mut StreamDbHandle) -> c_int {
    if handle.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        db.flush()
    }));
    
    match result {
        Ok(()) => STREAMDB_OK,
        Err(code) => code,
    }
}

/// Get database statistics.
///
/// # Safety
///
/// - `handle` must be a valid database handle
/// - `out_stats` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn streamdb_stats(
    handle: *mut StreamDbHandle,
    out_stats: *mut StreamDbStats,
) -> c_int {
    if handle.is_null() || out_stats.is_null() {
        return STREAMDB_ERR_INVALID;
    }
    
    let result = catch_panic(AssertUnwindSafe(|| {
        let db = unsafe { &(*handle).inner };
        db.stats()
    }));
    
    match result {
        Ok(stats) => {
            unsafe {
                *out_stats = StreamDbStats {
                    key_count: stats.key_count,
                    total_size: stats.total_size,
                    cache_hits: stats.cache_stats.hits,
                    cache_misses: stats.cache_stats.misses,
                };
            }
            STREAMDB_OK
        }
        Err(code) => code,
    }
}

/// Get the number of keys in the database.
///
/// # Safety
///
/// - `handle` must be a valid database handle
#[no_mangle]
pub unsafe extern "C" fn streamdb_len(handle: *mut StreamDbHandle) -> usize {
    if handle.is_null() {
        return 0;
    }
    
    let db = unsafe { &(*handle).inner };
    db.len()
}

/// Free a buffer returned by StreamDB.
///
/// # Safety
///
/// - `buffer` must be a valid pointer to a buffer from StreamDB
#[no_mangle]
pub unsafe extern "C" fn streamdb_free_buffer(buffer: *mut StreamDbBuffer) {
    if buffer.is_null() {
        return;
    }
    
    let buf = unsafe { &*buffer };
    
    if !buf.data.is_null() && buf.capacity > 0 {
        let _ = unsafe {
            Vec::from_raw_parts(buf.data, buf.len, buf.capacity)
        };
    }
    
    unsafe {
        *buffer = StreamDbBuffer::null();
    }
}

/// Get the library version string.
///
/// # Safety
///
/// - The returned string is statically allocated and must not be freed
#[no_mangle]
pub extern "C" fn streamdb_version() -> *const c_char {
    static VERSION_CSTR: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION_CSTR.as_ptr() as *const c_char
}

// ============================================================================
// C Header Generation
// ============================================================================

/// Generate C header content (for cbindgen)
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ffi_basic_flow() {
        unsafe {
            let mut handle: *mut StreamDbHandle = ptr::null_mut();
            
            // Open memory database
            let status = streamdb_open_memory(&mut handle);
            assert_eq!(status, STREAMDB_OK);
            assert!(!handle.is_null());
            
            // Insert
            let key = b"test_key";
            let value = b"test_value";
            let mut id = StreamDbUuid { bytes: [0; 16] };
            
            let status = streamdb_insert(
                handle,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
                &mut id,
            );
            assert_eq!(status, STREAMDB_OK);
            
            // Get
            let mut buffer = StreamDbBuffer::null();
            let status = streamdb_get(handle, key.as_ptr(), key.len(), &mut buffer);
            assert_eq!(status, STREAMDB_OK);
            assert!(!buffer.data.is_null());
            assert_eq!(buffer.len, value.len());
            
            let retrieved = slice::from_raw_parts(buffer.data, buffer.len);
            assert_eq!(retrieved, value);
            
            streamdb_free_buffer(&mut buffer);
            
            // Exists
            let mut exists: c_int = 0;
            let status = streamdb_exists(handle, key.as_ptr(), key.len(), &mut exists);
            assert_eq!(status, STREAMDB_OK);
            assert_eq!(exists, 1);
            
            // Delete
            let status = streamdb_delete(handle, key.as_ptr(), key.len());
            assert_eq!(status, STREAMDB_OK);
            
            // Verify deleted
            let status = streamdb_exists(handle, key.as_ptr(), key.len(), &mut exists);
            assert_eq!(status, STREAMDB_OK);
            assert_eq!(exists, 0);
            
            // Close
            streamdb_close(handle);
        }
    }
    
    #[test]
    fn test_ffi_null_safety() {
        unsafe {
            // Null handle
            assert_eq!(streamdb_flush(ptr::null_mut()), STREAMDB_ERR_INVALID);
            
            // Null path
            let mut handle: *mut StreamDbHandle = ptr::null_mut();
            assert_eq!(streamdb_open(ptr::null(), &mut handle), STREAMDB_ERR_INVALID);
            
            // Close null (should be safe)
            streamdb_close(ptr::null_mut());
        }
    }
    
    #[test]
    fn test_ffi_suffix_search() {
        unsafe {
            let mut handle: *mut StreamDbHandle = ptr::null_mut();
            streamdb_open_memory(&mut handle);
            
            // Insert test data
            streamdb_insert(handle, b"user:alice".as_ptr(), 10, b"Alice".as_ptr(), 5, ptr::null_mut());
            streamdb_insert(handle, b"admin:alice".as_ptr(), 11, b"Admin".as_ptr(), 5, ptr::null_mut());
            streamdb_insert(handle, b"user:bob".as_ptr(), 8, b"Bob".as_ptr(), 3, ptr::null_mut());
            
            // Search
            let mut results = StreamDbSearchResults {
                results: ptr::null_mut(),
                count: 0,
            };
            
            let status = streamdb_suffix_search(
                handle,
                b"alice".as_ptr(),
                5,
                &mut results,
            );
            assert_eq!(status, STREAMDB_OK);
            assert_eq!(results.count, 2);
            
            streamdb_free_search_results(&mut results);
            streamdb_close(handle);
        }
    }
}
