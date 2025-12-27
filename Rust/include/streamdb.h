/*
 * StreamDB - C API Header
 *
 * Copyright (C) 2025 DeMoD LLC
 * Licensed under LGPL-2.1-or-later
 *
 * This file provides the C interface for the StreamDB Rust library.
 */

#ifndef STREAMDB_H
#define STREAMDB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Status Codes
 * ============================================================================ */

/** Operation succeeded */
#define STREAMDB_OK           0
/** I/O error */
#define STREAMDB_ERR_IO      -1
/** Key/document not found */
#define STREAMDB_ERR_NOT_FOUND -2
/** Invalid input (null pointer, invalid UTF-8, etc.) */
#define STREAMDB_ERR_INVALID -3
/** Internal error (panic caught) */
#define STREAMDB_ERR_INTERNAL -4
/** Data corruption detected */
#define STREAMDB_ERR_CORRUPTED -5

/* ============================================================================
 * Types
 * ============================================================================ */

/** Opaque handle to a StreamDB instance */
typedef struct StreamDbHandle StreamDbHandle;

/** Buffer for binary data */
typedef struct {
    uint8_t* data;    /**< Pointer to data (must be freed with streamdb_free_buffer) */
    size_t len;       /**< Length in bytes */
    size_t capacity;  /**< Internal use only */
} StreamDbBuffer;

/** UUID (128-bit) */
typedef struct {
    uint8_t bytes[16]; /**< UUID bytes in big-endian order */
} StreamDbUuid;

/** Search result entry */
typedef struct {
    uint8_t* key;     /**< Key bytes (owned, freed with streamdb_free_search_results) */
    size_t key_len;   /**< Key length */
    StreamDbUuid id;  /**< Document UUID */
} StreamDbSearchResult;

/** Array of search results */
typedef struct {
    StreamDbSearchResult* results; /**< Array of results */
    size_t count;                  /**< Number of results */
} StreamDbSearchResults;

/** Database statistics */
typedef struct {
    size_t key_count;    /**< Number of keys */
    uint64_t total_size; /**< Total value size in bytes */
    uint64_t cache_hits; /**< Cache hit count */
    uint64_t cache_misses; /**< Cache miss count */
} StreamDbStats;

/* ============================================================================
 * Database Lifecycle
 * ============================================================================ */

/**
 * Open or create a database file.
 *
 * @param path Path to the database file (UTF-8, null-terminated)
 * @param out_handle Pointer to receive the database handle
 * @return STREAMDB_OK on success, error code on failure
 */
int streamdb_open(const char* path, StreamDbHandle** out_handle);

/**
 * Open an in-memory database (no persistence).
 *
 * @param out_handle Pointer to receive the database handle
 * @return STREAMDB_OK on success, error code on failure
 */
int streamdb_open_memory(StreamDbHandle** out_handle);

/**
 * Close a database and free all resources.
 *
 * @param handle Database handle (safe to pass NULL)
 */
void streamdb_close(StreamDbHandle* handle);

/* ============================================================================
 * Core Operations
 * ============================================================================ */

/**
 * Insert or update a key-value pair.
 *
 * @param handle Database handle
 * @param key Key bytes
 * @param key_len Key length
 * @param value Value bytes
 * @param value_len Value length
 * @param out_id Pointer to receive document UUID (may be NULL)
 * @return STREAMDB_OK on success, error code on failure
 */
int streamdb_insert(
    StreamDbHandle* handle,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    StreamDbUuid* out_id
);

/**
 * Get a value by key.
 *
 * @param handle Database handle
 * @param key Key bytes
 * @param key_len Key length
 * @param out_buffer Pointer to receive the value buffer
 * @return STREAMDB_OK on success, STREAMDB_ERR_NOT_FOUND if not found
 *
 * @note The returned buffer must be freed with streamdb_free_buffer()
 */
int streamdb_get(
    StreamDbHandle* handle,
    const uint8_t* key,
    size_t key_len,
    StreamDbBuffer* out_buffer
);

/**
 * Check if a key exists.
 *
 * @param handle Database handle
 * @param key Key bytes
 * @param key_len Key length
 * @param out_exists Pointer to receive existence flag (1 or 0)
 * @return STREAMDB_OK on success, error code on failure
 */
int streamdb_exists(
    StreamDbHandle* handle,
    const uint8_t* key,
    size_t key_len,
    int* out_exists
);

/**
 * Delete a key-value pair.
 *
 * @param handle Database handle
 * @param key Key bytes
 * @param key_len Key length
 * @return STREAMDB_OK on success, STREAMDB_ERR_NOT_FOUND if not found
 */
int streamdb_delete(
    StreamDbHandle* handle,
    const uint8_t* key,
    size_t key_len
);

/* ============================================================================
 * Search Operations
 * ============================================================================ */

/**
 * Search for keys ending with the given suffix.
 *
 * @param handle Database handle
 * @param suffix Suffix bytes
 * @param suffix_len Suffix length
 * @param out_results Pointer to receive search results
 * @return STREAMDB_OK on success, error code on failure
 *
 * @note The returned results must be freed with streamdb_free_search_results()
 */
int streamdb_suffix_search(
    StreamDbHandle* handle,
    const uint8_t* suffix,
    size_t suffix_len,
    StreamDbSearchResults* out_results
);

/**
 * Free search results.
 *
 * @param results Pointer to search results (safe to pass NULL contents)
 */
void streamdb_free_search_results(StreamDbSearchResults* results);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * Flush pending writes to disk.
 *
 * @param handle Database handle
 * @return STREAMDB_OK on success, error code on failure
 */
int streamdb_flush(StreamDbHandle* handle);

/**
 * Get database statistics.
 *
 * @param handle Database handle
 * @param out_stats Pointer to receive statistics
 * @return STREAMDB_OK on success, error code on failure
 */
int streamdb_stats(StreamDbHandle* handle, StreamDbStats* out_stats);

/**
 * Get the number of keys in the database.
 *
 * @param handle Database handle
 * @return Number of keys (0 if handle is NULL)
 */
size_t streamdb_len(StreamDbHandle* handle);

/**
 * Free a buffer returned by StreamDB.
 *
 * @param buffer Pointer to buffer (safe to pass NULL)
 */
void streamdb_free_buffer(StreamDbBuffer* buffer);

/**
 * Get the library version string.
 *
 * @return Version string (statically allocated, do not free)
 */
const char* streamdb_version(void);

#ifdef __cplusplus
}
#endif

#endif /* STREAMDB_H */
