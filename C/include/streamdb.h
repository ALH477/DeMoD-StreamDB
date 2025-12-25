/*
 * StreamDB - A lightweight, thread-safe embedded database using reverse trie
 * 
 * Copyright (C) 2025 DeMoD LLC
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 * 
 * Contact: DeMoD LLC
 */

/**
 * @file streamdb.h
 * @brief StreamDB Public API
 * 
 * StreamDB is an in-memory key-value store with optional file persistence.
 * It uses a reverse trie structure for efficient suffix-based searches.
 * 
 * ## Features
 * - Thread-safe: All operations use internal mutex locking
 * - Binary keys: Supports arbitrary byte sequences (not just strings)
 * - Auto-flush: Background thread periodically saves to disk
 * - Cross-platform: Windows, Linux, macOS, Unix
 * - Memory-only mode: Pass NULL as file_path to streamdb_init()
 * 
 * ## Basic Example
 * @code
 *   StreamDB* db = streamdb_init("mydb.dat", 5000);  // File backend, 5s flush
 *   streamdb_insert(db, (unsigned char*)"key", 3, "value", 6);
 *   
 *   size_t size;
 *   void* val = streamdb_get(db, (unsigned char*)"key", 3, &size);
 *   if (val) {
 *     // Use val...
 *     free(val);  // Important: get() returns a copy that must be freed
 *   }
 *   
 *   streamdb_delete(db, (unsigned char*)"key", 3);
 *   streamdb_free(db);  // Auto-flushes before cleanup
 * @endcode
 * 
 * ## Suffix Search Example
 * @code
 *   StreamDBResult* results = streamdb_suffix_search(db, (unsigned char*)"suffix", 6);
 *   for (StreamDBResult* r = results; r; r = r->next) {
 *     // Process r->key, r->value...
 *   }
 *   streamdb_free_results(results);
 * @endcode
 * 
 * ## Thread Safety
 * All public functions are thread-safe. Values returned by streamdb_get()
 * are copies and must be freed by the caller.
 * 
 * IMPORTANT: streamdb_free() must only be called when no other threads
 * are accessing the database. The caller must ensure proper synchronization.
 */

#ifndef STREAMDB_H
#define STREAMDB_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version information */
#define STREAMDB_VERSION_MAJOR 2
#define STREAMDB_VERSION_MINOR 0
#define STREAMDB_VERSION_PATCH 0
#define STREAMDB_VERSION_STRING "2.0.0"

/* Configuration constants */
#define STREAMDB_MAX_KEY_LEN 1024
#define STREAMDB_DEFAULT_FLUSH_INTERVAL_MS 5000

/* Return codes */
typedef enum {
    STREAMDB_OK = 0,           /**< Operation successful */
    STREAMDB_ERROR = -1,       /**< General error */
    STREAMDB_NOT_FOUND = -2,   /**< Key not found */
    STREAMDB_INVALID_ARG = -3, /**< Invalid argument */
    STREAMDB_NO_MEMORY = -4,   /**< Memory allocation failed */
    STREAMDB_IO_ERROR = -5,    /**< File I/O error */
    STREAMDB_NOT_SUPPORTED = -6 /**< Operation not supported (e.g., flush on memory-only db) */
} StreamDBStatus;

/* Opaque database handle */
typedef struct StreamDB StreamDB;

/**
 * @brief Result structure for suffix searches
 * 
 * Results are returned as a linked list. Each result contains a copy
 * of the key and value that must be freed using streamdb_free_results().
 */
typedef struct StreamDBResult {
    unsigned char* key;          /**< Key bytes (copy, owned by result) */
    size_t key_len;              /**< Length of key in bytes */
    void* value;                 /**< Value data (copy, owned by result) */
    size_t value_size;           /**< Size of value in bytes */
    struct StreamDBResult* next; /**< Next result in list, or NULL */
} StreamDBResult;

/**
 * @brief Database statistics
 */
typedef struct StreamDBStats {
    size_t total_size;           /**< Total size of all values in bytes */
    size_t key_count;            /**< Number of keys stored */
    size_t node_count;           /**< Number of trie nodes allocated */
    int is_dirty;                /**< Non-zero if unflushed changes exist */
    int is_file_backend;         /**< Non-zero if file persistence is enabled */
} StreamDBStats;

/**
 * @brief Configuration for database initialization
 */
typedef struct StreamDBConfig {
    int flush_interval_ms;       /**< Auto-flush interval (0 to disable, default 5000) */
    int enable_compression;      /**< Reserved for future use */
    size_t max_memory;           /**< Reserved for future use (0 = unlimited) */
} StreamDBConfig;

/* ============================================================================
 * Initialization and Cleanup
 * ============================================================================ */

/**
 * @brief Initialize a new database instance
 * 
 * @param file_path Path to persistence file, or NULL for memory-only mode
 * @param flush_interval_ms Auto-flush interval in milliseconds (0 to disable)
 * @return Database handle, or NULL on failure
 * 
 * If file_path points to an existing file, its contents will be loaded.
 * The file will be created on first flush if it doesn't exist.
 */
StreamDB* streamdb_init(const char* file_path, int flush_interval_ms);

/**
 * @brief Initialize database with extended configuration
 * 
 * @param file_path Path to persistence file, or NULL for memory-only mode
 * @param config Configuration options, or NULL for defaults
 * @return Database handle, or NULL on failure
 */
StreamDB* streamdb_init_with_config(const char* file_path, const StreamDBConfig* config);

/**
 * @brief Free database and all associated resources
 * 
 * Performs a final flush if the database is dirty and file-backed.
 * 
 * IMPORTANT: Caller must ensure no other threads are accessing the database
 * when this function is called.
 * 
 * @param db Database handle (safe to pass NULL)
 */
void streamdb_free(StreamDB* db);

/* ============================================================================
 * Core Operations
 * ============================================================================ */

/**
 * @brief Insert or update a key-value pair
 * 
 * @param db Database handle
 * @param key Key bytes
 * @param key_len Length of key (1 to STREAMDB_MAX_KEY_LEN)
 * @param value Value data
 * @param value_size Size of value (must be > 0)
 * @return STREAMDB_OK on success, error code on failure
 */
StreamDBStatus streamdb_insert(StreamDB* db, const unsigned char* key, size_t key_len,
                                const void* value, size_t value_size);

/**
 * @brief Retrieve a value by key
 * 
 * Returns a COPY of the value for thread safety. The caller must free()
 * the returned pointer when done.
 * 
 * @param db Database handle
 * @param key Key bytes
 * @param key_len Length of key
 * @param[out] value_size Set to size of returned value, or 0 on failure
 * @return Copy of value (caller must free), or NULL if not found
 */
void* streamdb_get(StreamDB* db, const unsigned char* key, size_t key_len,
                   size_t* value_size);

/**
 * @brief Check if a key exists without copying the value
 * 
 * @param db Database handle
 * @param key Key bytes
 * @param key_len Length of key
 * @return 1 if key exists, 0 if not found or on error
 */
int streamdb_exists(StreamDB* db, const unsigned char* key, size_t key_len);

/**
 * @brief Delete a key-value pair
 * 
 * @param db Database handle
 * @param key Key bytes
 * @param key_len Length of key
 * @return STREAMDB_OK if deleted, STREAMDB_NOT_FOUND if key didn't exist,
 *         or other error code on failure
 */
StreamDBStatus streamdb_delete(StreamDB* db, const unsigned char* key, size_t key_len);

/* ============================================================================
 * Search Operations
 * ============================================================================ */

/**
 * @brief Search for all keys ending with the given suffix
 * 
 * The reverse trie structure makes suffix searches efficient.
 * Results are returned as a linked list of copies.
 * 
 * @param db Database handle
 * @param suffix Suffix bytes to search for
 * @param suffix_len Length of suffix
 * @return Linked list of results (caller must call streamdb_free_results),
 *         or NULL if no matches or on error
 */
StreamDBResult* streamdb_suffix_search(StreamDB* db, const unsigned char* suffix,
                                        size_t suffix_len);

/**
 * @brief Iterate over all key-value pairs
 * 
 * The callback receives copies of keys and values. Return 0 from callback
 * to continue iteration, non-zero to stop early.
 * 
 * @param db Database handle
 * @param callback Function called for each entry
 * @param user_data Opaque pointer passed to callback
 * @return STREAMDB_OK on success (or early stop), error code on failure
 */
typedef int (*streamdb_foreach_callback)(const unsigned char* key, size_t key_len,
                                          const void* value, size_t value_size,
                                          void* user_data);

StreamDBStatus streamdb_foreach(StreamDB* db, streamdb_foreach_callback callback,
                                 void* user_data);

/**
 * @brief Free a result list from streamdb_suffix_search
 * 
 * @param results Result list head (safe to pass NULL)
 */
void streamdb_free_results(StreamDBResult* results);

/* ============================================================================
 * Persistence Operations
 * ============================================================================ */

/**
 * @brief Flush database to disk
 * 
 * @param db Database handle
 * @return STREAMDB_OK on success, STREAMDB_NOT_SUPPORTED for memory-only db,
 *         or error code on failure
 */
StreamDBStatus streamdb_flush(StreamDB* db);

/**
 * @brief Request graceful shutdown of background threads
 * 
 * Signals the auto-flush thread to stop. Call this before streamdb_free()
 * to minimize shutdown delay.
 * 
 * @param db Database handle
 */
void streamdb_shutdown(StreamDB* db);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Get database statistics
 * 
 * @param db Database handle
 * @param[out] stats Statistics structure to fill
 * @return STREAMDB_OK on success, error code on failure
 */
StreamDBStatus streamdb_get_stats(StreamDB* db, StreamDBStats* stats);

/**
 * @brief Get human-readable error message
 * 
 * @param status Status code
 * @return Static string describing the error
 */
const char* streamdb_strerror(StreamDBStatus status);

/**
 * @brief Get library version string
 * 
 * @return Version string (e.g., "2.0.0")
 */
const char* streamdb_version(void);

/* Deprecated: Use streamdb_suffix_search instead */
#define streamdb_prefix_search streamdb_suffix_search

#ifdef __cplusplus
}
#endif

#endif /* STREAMDB_H */
