/*
 * StreamDB Wrapper - Extended API functions for StreamDB
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
 * @file streamdb_wrapper.h
 * @brief Extended wrapper functions for StreamDB
 * 
 * This module provides additional convenience functions built on top of
 * the core StreamDB API, including GUID generation for documents and
 * simplified search interfaces.
 */

#ifndef STREAMDB_WRAPPER_H
#define STREAMDB_WRAPPER_H

#include "streamdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Document Operations
 * ============================================================================ */

/**
 * @brief Write a document and receive a unique GUID
 * 
 * Stores the document at the given path and generates a unique identifier.
 * 
 * @param db Database handle
 * @param path Document path (used as key)
 * @param data Document content
 * @param size Size of document in bytes
 * @param[out] guid_out Buffer to receive GUID string (at least 37 bytes)
 * @return STREAMDB_OK on success, error code on failure
 * 
 * @note The GUID is written to guid_out buffer provided by caller.
 *       Buffer must be at least 37 bytes (36 chars + null terminator).
 */
StreamDBStatus streamdb_write_document(StreamDB* db, const char* path,
                                        const void* data, size_t size,
                                        char* guid_out);

/**
 * @brief Read a document by path
 * 
 * @param db Database handle
 * @param path Document path
 * @param[out] size Set to document size on success
 * @return Document data (caller must free), or NULL on failure
 */
void* streamdb_read_document(StreamDB* db, const char* path, size_t* size);

/* ============================================================================
 * Search Operations
 * ============================================================================ */

/**
 * @brief Search result list for wrapper functions
 */
typedef struct StreamDBSearchResults {
    char** paths;        /**< Array of path strings (each null-terminated) */
    size_t count;        /**< Number of paths in array */
} StreamDBSearchResults;

/**
 * @brief Search for paths ending with the given suffix
 * 
 * @param db Database handle
 * @param suffix Suffix string to search for
 * @return Search results (caller must call streamdb_free_search_results),
 *         or NULL on failure/no matches
 */
StreamDBSearchResults* streamdb_search(StreamDB* db, const char* suffix);

/**
 * @brief Free search results
 * 
 * @param results Results to free (safe to pass NULL)
 */
void streamdb_free_search_results(StreamDBSearchResults* results);

/* ============================================================================
 * Batch Operations
 * ============================================================================ */

/**
 * @brief Batch write entry
 */
typedef struct StreamDBBatchEntry {
    const char* path;
    const void* data;
    size_t size;
} StreamDBBatchEntry;

/**
 * @brief Write multiple documents in a batch
 * 
 * More efficient than individual writes as it only marks dirty once.
 * 
 * @param db Database handle
 * @param entries Array of entries to write
 * @param count Number of entries
 * @return STREAMDB_OK if all succeeded, first error code otherwise
 */
StreamDBStatus streamdb_batch_write(StreamDB* db, const StreamDBBatchEntry* entries,
                                     size_t count);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Generate a UUID v4 string
 * 
 * @param[out] uuid_out Buffer for UUID (at least 37 bytes)
 */
void streamdb_generate_uuid(char* uuid_out);

/* ============================================================================
 * Async Stubs (Not Implemented)
 * ============================================================================ */

/**
 * @brief Async operation callback type
 */
typedef void (*streamdb_async_callback)(StreamDBStatus status, void* result,
                                         void* user_data);

/**
 * @brief Async get (NOT IMPLEMENTED - returns STREAMDB_NOT_SUPPORTED)
 */
StreamDBStatus streamdb_get_async(StreamDB* db, const char* path,
                                   streamdb_async_callback callback,
                                   void* user_data);

/**
 * @brief Begin async transaction (NOT IMPLEMENTED)
 */
StreamDBStatus streamdb_begin_async_transaction(StreamDB* db,
                                                 streamdb_async_callback callback,
                                                 void* user_data);

/**
 * @brief Commit async transaction (NOT IMPLEMENTED)
 */
StreamDBStatus streamdb_commit_async_transaction(StreamDB* db,
                                                  streamdb_async_callback callback,
                                                  void* user_data);

/**
 * @brief Rollback async transaction (NOT IMPLEMENTED)
 */
StreamDBStatus streamdb_rollback_async_transaction(StreamDB* db,
                                                    streamdb_async_callback callback,
                                                    void* user_data);

/**
 * @brief Set quick mode (NO-OP in base implementation)
 */
void streamdb_set_quick_mode(StreamDB* db, int quick);

#ifdef __cplusplus
}
#endif

#endif /* STREAMDB_WRAPPER_H */
