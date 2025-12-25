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

/* Feature test macros - must be defined before any includes */
#if !defined(_WIN32) && !defined(_WIN64)
    #define _POSIX_C_SOURCE 200809L
    #define _DEFAULT_SOURCE
    #define _BSD_SOURCE
#endif

#include "streamdb_wrapper.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

/* ============================================================================
 * Platform-specific UUID generation
 * ============================================================================ */

#if defined(_WIN32) || defined(_WIN64)
    #include <windows.h>
    #include <rpc.h>
    #pragma comment(lib, "rpcrt4.lib")
    
    void streamdb_generate_uuid(char* uuid_out) {
        UUID uuid;
        if (UuidCreate(&uuid) == RPC_S_OK) {
            unsigned char* str;
            if (UuidToStringA(&uuid, &str) == RPC_S_OK) {
                strncpy(uuid_out, (char*)str, 36);
                uuid_out[36] = '\0';
                RpcStringFreeA(&str);
                return;
            }
        }
        /* Fallback to random UUID */
        goto fallback;
        
    fallback:
        {
            /* Generate random UUID v4 */
            static int seeded = 0;
            if (!seeded) {
                srand((unsigned int)time(NULL) ^ (unsigned int)GetCurrentProcessId());
                seeded = 1;
            }
            
            unsigned char bytes[16];
            for (int i = 0; i < 16; i++) {
                bytes[i] = (unsigned char)(rand() & 0xFF);
            }
            
            /* Set version 4 and variant bits */
            bytes[6] = (bytes[6] & 0x0F) | 0x40;
            bytes[8] = (bytes[8] & 0x3F) | 0x80;
            
            snprintf(uuid_out, 37, 
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9], bytes[10], bytes[11],
                bytes[12], bytes[13], bytes[14], bytes[15]);
        }
    }

#elif defined(__APPLE__)
    #include <uuid/uuid.h>
    
    void streamdb_generate_uuid(char* uuid_out) {
        uuid_t uuid;
        uuid_generate(uuid);
        uuid_unparse_lower(uuid, uuid_out);
    }

#elif defined(__linux__)
    #include <fcntl.h>
    #include <unistd.h>
    
    void streamdb_generate_uuid(char* uuid_out) {
        unsigned char bytes[16];
        int urandom = open("/dev/urandom", O_RDONLY);
        
        if (urandom >= 0) {
            ssize_t result = read(urandom, bytes, 16);
            close(urandom);
            
            if (result == 16) {
                /* Set version 4 and variant bits */
                bytes[6] = (bytes[6] & 0x0F) | 0x40;
                bytes[8] = (bytes[8] & 0x3F) | 0x80;
                
                snprintf(uuid_out, 37, 
                    "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                    bytes[12], bytes[13], bytes[14], bytes[15]);
                return;
            }
        }
        
        /* Fallback to time-based random */
        static int seeded = 0;
        if (!seeded) {
            srand((unsigned int)time(NULL) ^ (unsigned int)getpid());
            seeded = 1;
        }
        
        for (int i = 0; i < 16; i++) {
            bytes[i] = (unsigned char)(rand() & 0xFF);
        }
        
        bytes[6] = (bytes[6] & 0x0F) | 0x40;
        bytes[8] = (bytes[8] & 0x3F) | 0x80;
        
        snprintf(uuid_out, 37, 
            "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]);
    }

#else
    /* Generic fallback using rand() */
    void streamdb_generate_uuid(char* uuid_out) {
        static int seeded = 0;
        if (!seeded) {
            srand((unsigned int)time(NULL));
            seeded = 1;
        }
        
        unsigned char bytes[16];
        for (int i = 0; i < 16; i++) {
            bytes[i] = (unsigned char)(rand() & 0xFF);
        }
        
        /* Set version 4 and variant bits */
        bytes[6] = (bytes[6] & 0x0F) | 0x40;
        bytes[8] = (bytes[8] & 0x3F) | 0x80;
        
        snprintf(uuid_out, 37, 
            "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]);
    }
#endif

/* ============================================================================
 * Document Operations
 * ============================================================================ */

StreamDBStatus streamdb_write_document(StreamDB* db, const char* path,
                                        const void* data, size_t size,
                                        char* guid_out) {
    if (!db || !path || !data || size == 0 || !guid_out) {
        return STREAMDB_INVALID_ARG;
    }
    
    StreamDBStatus status = streamdb_insert(db, 
                                            (const unsigned char*)path,
                                            strlen(path),
                                            data, size);
    
    if (status == STREAMDB_OK) {
        streamdb_generate_uuid(guid_out);
    }
    
    return status;
}

void* streamdb_read_document(StreamDB* db, const char* path, size_t* size) {
    if (!db || !path || !size) {
        return NULL;
    }
    
    return streamdb_get(db, (const unsigned char*)path, strlen(path), size);
}

/* ============================================================================
 * Search Operations
 * ============================================================================ */

StreamDBSearchResults* streamdb_search(StreamDB* db, const char* suffix) {
    if (!db || !suffix) {
        return NULL;
    }
    
    size_t suffix_len = strlen(suffix);
    if (suffix_len == 0) {
        return NULL;
    }
    
    StreamDBResult* results = streamdb_suffix_search(db,
                                                      (const unsigned char*)suffix,
                                                      suffix_len);
    
    if (!results) {
        return NULL;
    }
    
    /* Count results */
    size_t count = 0;
    for (StreamDBResult* r = results; r; r = r->next) {
        count++;
    }
    
    /* Allocate search results structure */
    StreamDBSearchResults* search_results = 
        (StreamDBSearchResults*)malloc(sizeof(StreamDBSearchResults));
    if (!search_results) {
        streamdb_free_results(results);
        return NULL;
    }
    
    search_results->paths = (char**)calloc(count, sizeof(char*));
    if (!search_results->paths) {
        free(search_results);
        streamdb_free_results(results);
        return NULL;
    }
    
    search_results->count = 0;
    
    /* Copy paths to array */
    size_t i = 0;
    for (StreamDBResult* r = results; r && i < count; r = r->next) {
        search_results->paths[i] = (char*)malloc(r->key_len + 1);
        if (!search_results->paths[i]) {
            /* Cleanup on failure */
            streamdb_free_search_results(search_results);
            streamdb_free_results(results);
            return NULL;
        }
        
        memcpy(search_results->paths[i], r->key, r->key_len);
        search_results->paths[i][r->key_len] = '\0';
        search_results->count++;
        i++;
    }
    
    streamdb_free_results(results);
    return search_results;
}

void streamdb_free_search_results(StreamDBSearchResults* results) {
    if (!results) return;
    
    if (results->paths) {
        for (size_t i = 0; i < results->count; i++) {
            free(results->paths[i]);
        }
        free(results->paths);
    }
    
    free(results);
}

/* ============================================================================
 * Batch Operations
 * ============================================================================ */

StreamDBStatus streamdb_batch_write(StreamDB* db, const StreamDBBatchEntry* entries,
                                     size_t count) {
    if (!db || !entries || count == 0) {
        return STREAMDB_INVALID_ARG;
    }
    
    StreamDBStatus first_error = STREAMDB_OK;
    
    for (size_t i = 0; i < count; i++) {
        const StreamDBBatchEntry* entry = &entries[i];
        
        if (!entry->path || !entry->data || entry->size == 0) {
            if (first_error == STREAMDB_OK) {
                first_error = STREAMDB_INVALID_ARG;
            }
            continue;
        }
        
        StreamDBStatus status = streamdb_insert(db,
                                                (const unsigned char*)entry->path,
                                                strlen(entry->path),
                                                entry->data, entry->size);
        
        if (status != STREAMDB_OK && first_error == STREAMDB_OK) {
            first_error = status;
        }
    }
    
    return first_error;
}

/* ============================================================================
 * Async Stubs (Not Implemented)
 * ============================================================================ */

StreamDBStatus streamdb_get_async(StreamDB* db, const char* path,
                                   streamdb_async_callback callback,
                                   void* user_data) {
    (void)db;
    (void)path;
    (void)callback;
    (void)user_data;
    return STREAMDB_NOT_SUPPORTED;
}

StreamDBStatus streamdb_begin_async_transaction(StreamDB* db,
                                                 streamdb_async_callback callback,
                                                 void* user_data) {
    (void)db;
    (void)callback;
    (void)user_data;
    return STREAMDB_NOT_SUPPORTED;
}

StreamDBStatus streamdb_commit_async_transaction(StreamDB* db,
                                                  streamdb_async_callback callback,
                                                  void* user_data) {
    (void)db;
    (void)callback;
    (void)user_data;
    return STREAMDB_NOT_SUPPORTED;
}

StreamDBStatus streamdb_rollback_async_transaction(StreamDB* db,
                                                    streamdb_async_callback callback,
                                                    void* user_data) {
    (void)db;
    (void)callback;
    (void)user_data;
    return STREAMDB_NOT_SUPPORTED;
}

void streamdb_set_quick_mode(StreamDB* db, int quick) {
    /* No-op: base StreamDB doesn't have quick mode */
    (void)db;
    (void)quick;
}
