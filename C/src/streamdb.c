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

/* Feature test macros - must be defined before any includes */
#if !defined(_WIN32) && !defined(_WIN64)
    #define _POSIX_C_SOURCE 200809L
    #define _DEFAULT_SOURCE
    #define _BSD_SOURCE
#endif

#include "streamdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ============================================================================
 * Platform Abstraction Layer
 * ============================================================================ */

#if defined(_WIN32) || defined(_WIN64)
    #define STREAMDB_WINDOWS 1
    #include <windows.h>
    #include <process.h>
    
    typedef CRITICAL_SECTION mutex_t;
    typedef CONDITION_VARIABLE condvar_t;
    typedef HANDLE thread_t;
    
    static int mutex_init(mutex_t* m) {
        InitializeCriticalSection(m);
        return 0;
    }
    
    static void mutex_lock(mutex_t* m) {
        EnterCriticalSection(m);
    }
    
    static void mutex_unlock(mutex_t* m) {
        LeaveCriticalSection(m);
    }
    
    static void mutex_destroy(mutex_t* m) {
        DeleteCriticalSection(m);
    }
    
    static int condvar_init(condvar_t* cv) {
        InitializeConditionVariable(cv);
        return 0;
    }
    
    static void condvar_signal(condvar_t* cv) {
        WakeConditionVariable(cv);
    }
    
    static int condvar_timedwait(condvar_t* cv, mutex_t* m, int timeout_ms) {
        return SleepConditionVariableCS(cv, m, timeout_ms) ? 0 : -1;
    }
    
    static void condvar_destroy(condvar_t* cv) {
        (void)cv; /* No cleanup needed on Windows */
    }
    
    typedef unsigned (__stdcall *win_thread_func)(void*);
    
    static int thread_create(thread_t* t, void* (*func)(void*), void* arg) {
        *t = (HANDLE)_beginthreadex(NULL, 0, (win_thread_func)func, arg, 0, NULL);
        return (*t != NULL) ? 0 : -1;
    }
    
    static void thread_join(thread_t t) {
        WaitForSingleObject(t, INFINITE);
        CloseHandle(t);
    }
    
    static int get_pid(void) {
        return (int)GetCurrentProcessId();
    }

#elif defined(__unix__) || defined(__APPLE__) || defined(__linux__)
    #define STREAMDB_POSIX 1
    #include <pthread.h>
    #include <unistd.h>
    #include <errno.h>
    #include <sys/time.h>
    
    typedef pthread_mutex_t mutex_t;
    typedef pthread_cond_t condvar_t;
    typedef pthread_t thread_t;
    
    static int mutex_init(mutex_t* m) {
        pthread_mutexattr_t attr;
        int ret;
        
        ret = pthread_mutexattr_init(&attr);
        if (ret != 0) return ret;
        
        ret = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
        if (ret != 0) {
            pthread_mutexattr_destroy(&attr);
            return ret;
        }
        
        ret = pthread_mutex_init(m, &attr);
        pthread_mutexattr_destroy(&attr);
        return ret;
    }
    
    static void mutex_lock(mutex_t* m) {
        pthread_mutex_lock(m);
    }
    
    static void mutex_unlock(mutex_t* m) {
        pthread_mutex_unlock(m);
    }
    
    static void mutex_destroy(mutex_t* m) {
        pthread_mutex_destroy(m);
    }
    
    static int condvar_init(condvar_t* cv) {
        return pthread_cond_init(cv, NULL);
    }
    
    static void condvar_signal(condvar_t* cv) {
        pthread_cond_signal(cv);
    }
    
    static int condvar_timedwait(condvar_t* cv, mutex_t* m, int timeout_ms) {
        struct timespec ts;
        struct timeval tv;
        
        gettimeofday(&tv, NULL);
        ts.tv_sec = tv.tv_sec + timeout_ms / 1000;
        ts.tv_nsec = tv.tv_usec * 1000 + (timeout_ms % 1000) * 1000000;
        
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }
        
        return pthread_cond_timedwait(cv, m, &ts);
    }
    
    static void condvar_destroy(condvar_t* cv) {
        pthread_cond_destroy(cv);
    }
    
    static int thread_create(thread_t* t, void* (*func)(void*), void* arg) {
        return pthread_create(t, NULL, func, arg);
    }
    
    static void thread_join(thread_t t) {
        pthread_join(t, NULL);
    }
    
    static int get_pid(void) {
        return (int)getpid();
    }

#else
    /* Fallback: No threading support */
    #define STREAMDB_NO_THREADS 1
    
    typedef int mutex_t;
    typedef int condvar_t;
    typedef int thread_t;
    
    static int mutex_init(mutex_t* m) { *m = 0; return 0; }
    static void mutex_lock(mutex_t* m) { (void)m; }
    static void mutex_unlock(mutex_t* m) { (void)m; }
    static void mutex_destroy(mutex_t* m) { (void)m; }
    static int condvar_init(condvar_t* cv) { *cv = 0; return 0; }
    static void condvar_signal(condvar_t* cv) { (void)cv; }
    static int condvar_timedwait(condvar_t* cv, mutex_t* m, int timeout_ms) { 
        (void)cv; (void)m; (void)timeout_ms; return 0; 
    }
    static void condvar_destroy(condvar_t* cv) { (void)cv; }
    static int thread_create(thread_t* t, void* (*func)(void*), void* arg) { 
        (void)t; (void)func; (void)arg; return -1; 
    }
    static void thread_join(thread_t t) { (void)t; }
    static int get_pid(void) { return 0; }
#endif

/* ============================================================================
 * Internal Constants
 * ============================================================================ */

#define MAX_CHILDREN 256
#define SERIALIZE_STACK_SIZE 4096

/* ============================================================================
 * Internal Data Structures
 * ============================================================================ */

/**
 * Trie node structure
 * 
 * Uses sparse representation for memory efficiency: only allocated children
 * are stored, rather than a full 256-element array.
 */
typedef struct TrieNode {
    struct TrieNode* children[MAX_CHILDREN];
    void* value;
    size_t value_size;
    int is_end;
} TrieNode;

/**
 * Database structure
 */
struct StreamDB {
    TrieNode* root;
    size_t total_size;
    size_t key_count;
    size_t node_count;
    
    /* Threading */
    mutex_t mutex;
    condvar_t shutdown_cv;
    
    /* Persistence */
    char* file_path;
    int is_file_backend;
    int dirty;           /* Protected by mutex */
    int running;         /* Protected by mutex */
    int shutdown_requested; /* Protected by mutex */
    thread_t auto_thread;
    int auto_flush_interval_ms;
    int thread_started;
};

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

static TrieNode* create_node(StreamDB* db);
static void free_node(StreamDB* db, TrieNode* node);
static void free_node_recursive(StreamDB* db, TrieNode* node);
static int serialize_trie(FILE* fp, TrieNode* root);
static TrieNode* deserialize_trie(FILE* fp, StreamDB* db);
static void* auto_flush_thread(void* arg);
static StreamDBStatus internal_flush(StreamDB* db);
static void collect_all_nodes(TrieNode* node, unsigned char* key_buf, size_t key_len,
                              streamdb_foreach_callback callback, void* user_data,
                              int* should_stop);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

const char* streamdb_strerror(StreamDBStatus status) {
    switch (status) {
        case STREAMDB_OK:            return "Success";
        case STREAMDB_ERROR:         return "General error";
        case STREAMDB_NOT_FOUND:     return "Key not found";
        case STREAMDB_INVALID_ARG:   return "Invalid argument";
        case STREAMDB_NO_MEMORY:     return "Memory allocation failed";
        case STREAMDB_IO_ERROR:      return "File I/O error";
        case STREAMDB_NOT_SUPPORTED: return "Operation not supported";
        default:                     return "Unknown error";
    }
}

const char* streamdb_version(void) {
    return STREAMDB_VERSION_STRING;
}

/* ============================================================================
 * Node Management
 * ============================================================================ */

static TrieNode* create_node(StreamDB* db) {
    TrieNode* node = (TrieNode*)calloc(1, sizeof(TrieNode));
    if (node && db) {
        db->node_count++;
    }
    return node;
}

static void free_node(StreamDB* db, TrieNode* node) {
    if (!node) return;
    
    if (node->value) {
        free(node->value);
    }
    free(node);
    
    if (db) {
        db->node_count--;
    }
}

/* Iterative node freeing to avoid stack overflow */
static void free_node_recursive(StreamDB* db, TrieNode* node) {
    if (!node) return;
    
    /* Use iterative approach with explicit stack */
    TrieNode** stack = (TrieNode**)malloc(sizeof(TrieNode*) * SERIALIZE_STACK_SIZE);
    if (!stack) {
        /* Fallback to recursive if malloc fails (shouldn't happen in cleanup) */
        for (int i = 0; i < MAX_CHILDREN; i++) {
            free_node_recursive(db, node->children[i]);
        }
        free_node(db, node);
        return;
    }
    
    int stack_top = 0;
    stack[stack_top++] = node;
    
    while (stack_top > 0) {
        TrieNode* current = stack[--stack_top];
        
        /* Push children onto stack */
        for (int i = 0; i < MAX_CHILDREN; i++) {
            if (current->children[i]) {
                if (stack_top < SERIALIZE_STACK_SIZE) {
                    stack[stack_top++] = current->children[i];
                } else {
                    /* Stack overflow - fall back to recursive for this subtree */
                    free_node_recursive(db, current->children[i]);
                }
            }
        }
        
        free_node(db, current);
    }
    
    free(stack);
}

static int has_no_children(TrieNode* node) {
    for (int i = 0; i < MAX_CHILDREN; i++) {
        if (node->children[i]) return 0;
    }
    return 1;
}

/* ============================================================================
 * Serialization (Iterative to avoid stack overflow)
 * ============================================================================ */

typedef struct SerializeStackEntry {
    TrieNode* node;
    int child_index;  /* Next child to process, or -1 if node header not written */
} SerializeStackEntry;

static int serialize_trie(FILE* fp, TrieNode* root) {
    if (!root) return 0;
    
    SerializeStackEntry* stack = (SerializeStackEntry*)malloc(
        sizeof(SerializeStackEntry) * SERIALIZE_STACK_SIZE);
    if (!stack) return 0;
    
    int stack_top = 0;
    stack[stack_top].node = root;
    stack[stack_top].child_index = -1;
    stack_top++;
    
    int success = 1;
    
    while (stack_top > 0 && success) {
        SerializeStackEntry* entry = &stack[stack_top - 1];
        
        if (entry->child_index == -1) {
            /* Write node header */
            TrieNode* node = entry->node;
            
            if (fwrite(&node->is_end, sizeof(int), 1, fp) != 1) {
                success = 0;
                break;
            }
            if (fwrite(&node->value_size, sizeof(size_t), 1, fp) != 1) {
                success = 0;
                break;
            }
            
            if (node->is_end && node->value) {
                if (fwrite(node->value, 1, node->value_size, fp) != node->value_size) {
                    success = 0;
                    break;
                }
            }
            
            /* Count and write child count */
            int child_count = 0;
            for (int i = 0; i < MAX_CHILDREN; i++) {
                if (node->children[i]) child_count++;
            }
            if (fwrite(&child_count, sizeof(int), 1, fp) != 1) {
                success = 0;
                break;
            }
            
            entry->child_index = 0;
        }
        
        /* Find next child to process */
        TrieNode* node = entry->node;
        int found_child = 0;
        
        for (int i = entry->child_index; i < MAX_CHILDREN; i++) {
            if (node->children[i]) {
                /* Write child index */
                unsigned char byte = (unsigned char)i;
                if (fwrite(&byte, sizeof(unsigned char), 1, fp) != 1) {
                    success = 0;
                    break;
                }
                
                entry->child_index = i + 1;
                
                /* Push child onto stack */
                if (stack_top >= SERIALIZE_STACK_SIZE) {
                    fprintf(stderr, "StreamDB: Serialize stack overflow\n");
                    success = 0;
                    break;
                }
                
                stack[stack_top].node = node->children[i];
                stack[stack_top].child_index = -1;
                stack_top++;
                found_child = 1;
                break;
            }
        }
        
        if (!found_child && success) {
            /* No more children, pop this node */
            stack_top--;
        }
    }
    
    free(stack);
    return success;
}

static TrieNode* deserialize_trie(FILE* fp, StreamDB* db) {
    typedef struct {
        TrieNode* node;
        int remaining_children;
    } DeserializeEntry;
    
    DeserializeEntry* stack = (DeserializeEntry*)malloc(
        sizeof(DeserializeEntry) * SERIALIZE_STACK_SIZE);
    if (!stack) return NULL;
    
    TrieNode* root = NULL;
    int stack_top = 0;
    
    /* Read root node */
    root = create_node(db);
    if (!root) {
        free(stack);
        return NULL;
    }
    
    if (fread(&root->is_end, sizeof(int), 1, fp) != 1) goto fail;
    if (fread(&root->value_size, sizeof(size_t), 1, fp) != 1) goto fail;
    
    if (root->is_end && root->value_size > 0) {
        root->value = malloc(root->value_size);
        if (!root->value) goto fail;
        if (fread(root->value, 1, root->value_size, fp) != root->value_size) goto fail;
        db->key_count++;
        db->total_size += root->value_size;
    }
    
    int child_count;
    if (fread(&child_count, sizeof(int), 1, fp) != 1) goto fail;
    
    if (child_count > 0) {
        stack[stack_top].node = root;
        stack[stack_top].remaining_children = child_count;
        stack_top++;
    }
    
    while (stack_top > 0) {
        DeserializeEntry* entry = &stack[stack_top - 1];
        
        if (entry->remaining_children == 0) {
            stack_top--;
            continue;
        }
        
        /* Read child index */
        unsigned char byte;
        if (fread(&byte, sizeof(unsigned char), 1, fp) != 1) goto fail;
        
        /* Create child node */
        TrieNode* child = create_node(db);
        if (!child) goto fail;
        
        entry->node->children[byte] = child;
        entry->remaining_children--;
        
        /* Read child data */
        if (fread(&child->is_end, sizeof(int), 1, fp) != 1) goto fail;
        if (fread(&child->value_size, sizeof(size_t), 1, fp) != 1) goto fail;
        
        if (child->is_end && child->value_size > 0) {
            child->value = malloc(child->value_size);
            if (!child->value) goto fail;
            if (fread(child->value, 1, child->value_size, fp) != child->value_size) goto fail;
            db->key_count++;
            db->total_size += child->value_size;
        }
        
        int child_child_count;
        if (fread(&child_child_count, sizeof(int), 1, fp) != 1) goto fail;
        
        if (child_child_count > 0) {
            if (stack_top >= SERIALIZE_STACK_SIZE) {
                fprintf(stderr, "StreamDB: Deserialize stack overflow\n");
                goto fail;
            }
            stack[stack_top].node = child;
            stack[stack_top].remaining_children = child_child_count;
            stack_top++;
        }
    }
    
    free(stack);
    return root;
    
fail:
    free(stack);
    if (root) {
        free_node_recursive(db, root);
    }
    return NULL;
}

/* ============================================================================
 * Persistence
 * ============================================================================ */

static StreamDBStatus internal_flush(StreamDB* db) {
    if (!db->is_file_backend || !db->file_path) {
        return STREAMDB_NOT_SUPPORTED;
    }
    
    /* Create temp file with PID to avoid collisions */
    char temp_path[1100];
    snprintf(temp_path, sizeof(temp_path), "%s.%d.tmp", db->file_path, get_pid());
    
    FILE* fp = fopen(temp_path, "wb");
    if (!fp) {
        return STREAMDB_IO_ERROR;
    }
    
    /* Write magic number and version for future compatibility */
    const char magic[] = "STDB";
    int version = 2;
    fwrite(magic, 1, 4, fp);
    fwrite(&version, sizeof(int), 1, fp);
    
    int success = serialize_trie(fp, db->root);
    fclose(fp);
    
    if (success) {
        /* Atomic rename */
        #ifdef STREAMDB_WINDOWS
        /* Windows doesn't support atomic rename over existing file */
        DeleteFileA(db->file_path);
        #else
        /* POSIX rename is atomic */
        #endif
        
        if (rename(temp_path, db->file_path) != 0) {
            remove(temp_path);
            return STREAMDB_IO_ERROR;
        }
        return STREAMDB_OK;
    } else {
        remove(temp_path);
        return STREAMDB_IO_ERROR;
    }
}

StreamDBStatus streamdb_flush(StreamDB* db) {
    if (!db) return STREAMDB_INVALID_ARG;
    if (!db->is_file_backend) return STREAMDB_NOT_SUPPORTED;
    
    mutex_lock(&db->mutex);
    StreamDBStatus status = internal_flush(db);
    if (status == STREAMDB_OK) {
        db->dirty = 0;
    }
    mutex_unlock(&db->mutex);
    
    return status;
}

/* Background auto-flush thread */
static void* auto_flush_thread(void* arg) {
    StreamDB* db = (StreamDB*)arg;
    
    mutex_lock(&db->mutex);
    
    while (db->running && !db->shutdown_requested) {
        /* Wait with timeout for shutdown signal or timeout */
        condvar_timedwait(&db->shutdown_cv, &db->mutex, db->auto_flush_interval_ms);
        
        if (db->shutdown_requested) break;
        
        if (db->dirty) {
            StreamDBStatus status = internal_flush(db);
            if (status == STREAMDB_OK) {
                db->dirty = 0;
            }
        }
    }
    
    mutex_unlock(&db->mutex);
    return NULL;
}

/* ============================================================================
 * Initialization and Cleanup
 * ============================================================================ */

StreamDB* streamdb_init(const char* file_path, int flush_interval_ms) {
    StreamDBConfig config = {0};
    config.flush_interval_ms = flush_interval_ms;
    return streamdb_init_with_config(file_path, &config);
}

StreamDB* streamdb_init_with_config(const char* file_path, const StreamDBConfig* config) {
    StreamDB* db = (StreamDB*)calloc(1, sizeof(StreamDB));
    if (!db) return NULL;
    
    db->root = create_node(db);
    if (!db->root) {
        free(db);
        return NULL;
    }
    
    db->total_size = 0;
    db->key_count = 0;
    db->file_path = file_path ? strdup(file_path) : NULL;
    db->is_file_backend = (file_path != NULL);
    db->dirty = 0;
    db->running = 1;
    db->shutdown_requested = 0;
    db->thread_started = 0;
    
    int flush_ms = config ? config->flush_interval_ms : STREAMDB_DEFAULT_FLUSH_INTERVAL_MS;
    db->auto_flush_interval_ms = flush_ms > 0 ? flush_ms : STREAMDB_DEFAULT_FLUSH_INTERVAL_MS;
    
    if (mutex_init(&db->mutex) != 0) {
        free_node(db, db->root);
        free(db->file_path);
        free(db);
        return NULL;
    }
    
    if (condvar_init(&db->shutdown_cv) != 0) {
        mutex_destroy(&db->mutex);
        free_node(db, db->root);
        free(db->file_path);
        free(db);
        return NULL;
    }
    
    /* Load from file if exists */
    if (db->is_file_backend && db->file_path) {
        FILE* fp = fopen(db->file_path, "rb");
        if (fp) {
            /* Check magic number */
            char magic[4];
            int version;
            
            if (fread(magic, 1, 4, fp) == 4 && memcmp(magic, "STDB", 4) == 0) {
                if (fread(&version, sizeof(int), 1, fp) == 1) {
                    if (version == 2) {
                        TrieNode* loaded = deserialize_trie(fp, db);
                        if (loaded) {
                            free_node(db, db->root);
                            db->root = loaded;
                            db->dirty = 0;
                        }
                    } else {
                        fprintf(stderr, "StreamDB: Unsupported file version %d\n", version);
                    }
                }
            } else {
                /* Try legacy format (no header) */
                rewind(fp);
                TrieNode* loaded = deserialize_trie(fp, db);
                if (loaded) {
                    free_node(db, db->root);
                    db->root = loaded;
                    db->dirty = 0;
                }
            }
            fclose(fp);
        }
        
        /* Start auto-flush thread */
        #ifndef STREAMDB_NO_THREADS
        if (flush_ms > 0) {
            if (thread_create(&db->auto_thread, auto_flush_thread, db) == 0) {
                db->thread_started = 1;
            } else {
                fprintf(stderr, "StreamDB: Failed to start auto-flush thread\n");
            }
        }
        #endif
    }
    
    return db;
}

void streamdb_shutdown(StreamDB* db) {
    if (!db) return;
    
    mutex_lock(&db->mutex);
    db->shutdown_requested = 1;
    condvar_signal(&db->shutdown_cv);
    mutex_unlock(&db->mutex);
}

void streamdb_free(StreamDB* db) {
    if (!db) return;
    
    /* Signal shutdown and wait for thread */
    mutex_lock(&db->mutex);
    db->running = 0;
    db->shutdown_requested = 1;
    condvar_signal(&db->shutdown_cv);
    mutex_unlock(&db->mutex);
    
    #ifndef STREAMDB_NO_THREADS
    if (db->thread_started) {
        thread_join(db->auto_thread);
    }
    #endif
    
    /* Final flush if dirty */
    mutex_lock(&db->mutex);
    if (db->is_file_backend && db->dirty) {
        internal_flush(db);
    }
    mutex_unlock(&db->mutex);
    
    /* Free trie */
    free_node_recursive(db, db->root);
    
    /* Cleanup */
    condvar_destroy(&db->shutdown_cv);
    mutex_destroy(&db->mutex);
    free(db->file_path);
    free(db);
}

/* ============================================================================
 * Core Operations
 * ============================================================================ */

StreamDBStatus streamdb_insert(StreamDB* db, const unsigned char* key, size_t key_len,
                                const void* value, size_t value_size) {
    if (!db || !key || key_len == 0 || !value || value_size == 0) {
        return STREAMDB_INVALID_ARG;
    }
    if (key_len > STREAMDB_MAX_KEY_LEN) {
        return STREAMDB_INVALID_ARG;
    }
    
    mutex_lock(&db->mutex);
    
    TrieNode* current = db->root;
    
    /* Traverse/create nodes in reverse order */
    for (int i = (int)key_len - 1; i >= 0; i--) {
        unsigned char c = key[i];
        if (!current->children[c]) {
            current->children[c] = create_node(db);
            if (!current->children[c]) {
                mutex_unlock(&db->mutex);
                return STREAMDB_NO_MEMORY;
            }
        }
        current = current->children[c];
    }
    
    /* Update value at leaf */
    int is_new_key = !current->is_end;
    
    if (current->is_end && current->value) {
        db->total_size -= current->value_size;
        free(current->value);
    }
    
    current->value = malloc(value_size);
    if (!current->value) {
        mutex_unlock(&db->mutex);
        return STREAMDB_NO_MEMORY;
    }
    
    memcpy(current->value, value, value_size);
    current->value_size = value_size;
    current->is_end = 1;
    db->total_size += value_size;
    
    if (is_new_key) {
        db->key_count++;
    }
    
    db->dirty = 1;
    
    mutex_unlock(&db->mutex);
    return STREAMDB_OK;
}

/* Internal get without mutex (for use within locked sections) */
static TrieNode* internal_find(StreamDB* db, const unsigned char* key, size_t key_len) {
    if (!db || !key || key_len == 0 || key_len > STREAMDB_MAX_KEY_LEN) {
        return NULL;
    }
    
    TrieNode* current = db->root;
    
    /* Traverse in reverse order */
    for (int i = (int)key_len - 1; i >= 0; i--) {
        unsigned char c = key[i];
        if (!current->children[c]) {
            return NULL;
        }
        current = current->children[c];
    }
    
    return current->is_end ? current : NULL;
}

void* streamdb_get(StreamDB* db, const unsigned char* key, size_t key_len,
                   size_t* value_size) {
    if (!value_size) return NULL;
    *value_size = 0;
    
    if (!db || !key || key_len == 0) return NULL;
    
    mutex_lock(&db->mutex);
    
    TrieNode* node = internal_find(db, key, key_len);
    void* result = NULL;
    
    if (node && node->value) {
        result = malloc(node->value_size);
        if (result) {
            memcpy(result, node->value, node->value_size);
            *value_size = node->value_size;
        }
    }
    
    mutex_unlock(&db->mutex);
    return result;
}

int streamdb_exists(StreamDB* db, const unsigned char* key, size_t key_len) {
    if (!db || !key || key_len == 0) return 0;
    
    mutex_lock(&db->mutex);
    TrieNode* node = internal_find(db, key, key_len);
    int exists = (node != NULL);
    mutex_unlock(&db->mutex);
    
    return exists;
}

/* Recursive helper for delete - never deletes the root */
static TrieNode* remove_helper(StreamDB* db, TrieNode* node, const unsigned char* key,
                                size_t key_len, size_t index, size_t* removed_size,
                                int is_root) {
    if (!node) return NULL;
    
    if (index == key_len) {
        if (node->is_end) {
            *removed_size = node->value_size;
            free(node->value);
            node->value = NULL;
            node->value_size = 0;
            node->is_end = 0;
            db->key_count--;
        }
        
        /* Never delete the root node */
        if (has_no_children(node) && !node->is_end && !is_root) {
            free_node(db, node);
            return NULL;
        }
        return node;
    }
    
    unsigned char c = key[key_len - 1 - index];
    node->children[c] = remove_helper(db, node->children[c], key, key_len,
                                       index + 1, removed_size, 0);
    
    /* Never delete the root node */
    if (has_no_children(node) && !node->is_end && !is_root) {
        free_node(db, node);
        return NULL;
    }
    return node;
}

StreamDBStatus streamdb_delete(StreamDB* db, const unsigned char* key, size_t key_len) {
    if (!db || !key || key_len == 0) {
        return STREAMDB_INVALID_ARG;
    }
    if (key_len > STREAMDB_MAX_KEY_LEN) {
        return STREAMDB_INVALID_ARG;
    }
    
    mutex_lock(&db->mutex);
    
    size_t removed_size = 0;
    db->root = remove_helper(db, db->root, key, key_len, 0, &removed_size, 1);
    
    /* Ensure root is never NULL */
    if (!db->root) {
        db->root = create_node(db);
    }
    
    StreamDBStatus status;
    if (removed_size > 0) {
        db->total_size -= removed_size;
        db->dirty = 1;
        status = STREAMDB_OK;
    } else {
        status = STREAMDB_NOT_FOUND;
    }
    
    mutex_unlock(&db->mutex);
    return status;
}

/* ============================================================================
 * Search Operations
 * ============================================================================ */

/* Helper for suffix search to collect results */
static void collect_results(TrieNode* node, unsigned char* extension, size_t ext_len,
                            const unsigned char* rev_suffix, size_t suffix_len,
                            StreamDBResult** results, int* error) {
    if (!node || *error) return;
    
    if (node->is_end) {
        size_t full_len = suffix_len + ext_len;
        if (full_len > STREAMDB_MAX_KEY_LEN) return;
        
        /* Build the full reversed key */
        unsigned char* full_rev = (unsigned char*)malloc(full_len);
        if (!full_rev) {
            *error = 1;
            return;
        }
        memcpy(full_rev, rev_suffix, suffix_len);
        memcpy(full_rev + suffix_len, extension, ext_len);
        
        /* Reverse to get original key */
        unsigned char* key = (unsigned char*)malloc(full_len);
        if (!key) {
            free(full_rev);
            *error = 1;
            return;
        }
        for (size_t j = 0; j < full_len; j++) {
            key[j] = full_rev[full_len - 1 - j];
        }
        free(full_rev);
        
        /* Create result */
        StreamDBResult* res = (StreamDBResult*)malloc(sizeof(StreamDBResult));
        if (!res) {
            free(key);
            *error = 1;
            return;
        }
        
        res->key = key;
        res->key_len = full_len;
        res->value = malloc(node->value_size);
        if (!res->value) {
            free(key);
            free(res);
            *error = 1;
            return;
        }
        memcpy(res->value, node->value, node->value_size);
        res->value_size = node->value_size;
        res->next = *results;
        *results = res;
    }
    
    for (int i = 0; i < MAX_CHILDREN; i++) {
        if (node->children[i]) {
            if (ext_len >= STREAMDB_MAX_KEY_LEN - suffix_len) continue;
            extension[ext_len] = (unsigned char)i;
            collect_results(node->children[i], extension, ext_len + 1,
                           rev_suffix, suffix_len, results, error);
        }
    }
}

StreamDBResult* streamdb_suffix_search(StreamDB* db, const unsigned char* suffix,
                                        size_t suffix_len) {
    if (!db || !suffix || suffix_len == 0 || suffix_len > STREAMDB_MAX_KEY_LEN) {
        return NULL;
    }
    
    mutex_lock(&db->mutex);
    
    TrieNode* current = db->root;
    
    /* Build reversed suffix and traverse */
    unsigned char* rev_suffix = (unsigned char*)malloc(suffix_len);
    if (!rev_suffix) {
        mutex_unlock(&db->mutex);
        return NULL;
    }
    
    for (size_t i = 0; i < suffix_len; i++) {
        unsigned char c = suffix[suffix_len - 1 - i];
        rev_suffix[i] = c;
        if (!current->children[c]) {
            free(rev_suffix);
            mutex_unlock(&db->mutex);
            return NULL;
        }
        current = current->children[c];
    }
    
    StreamDBResult* results = NULL;
    int error = 0;
    
    unsigned char* extension = (unsigned char*)malloc(STREAMDB_MAX_KEY_LEN);
    if (extension) {
        collect_results(current, extension, 0, rev_suffix, suffix_len, &results, &error);
        free(extension);
    }
    
    free(rev_suffix);
    
    if (error) {
        streamdb_free_results(results);
        results = NULL;
    }
    
    mutex_unlock(&db->mutex);
    return results;
}

void streamdb_free_results(StreamDBResult* results) {
    while (results) {
        StreamDBResult* next = results->next;
        free(results->key);
        free(results->value);
        free(results);
        results = next;
    }
}

/* ============================================================================
 * Iteration
 * ============================================================================ */

static void collect_all_nodes(TrieNode* node, unsigned char* key_buf, size_t key_len,
                              streamdb_foreach_callback callback, void* user_data,
                              int* should_stop) {
    if (!node || *should_stop) return;
    
    if (node->is_end) {
        /* Reverse the key buffer to get original key */
        unsigned char* key = (unsigned char*)malloc(key_len);
        if (key) {
            for (size_t i = 0; i < key_len; i++) {
                key[i] = key_buf[key_len - 1 - i];
            }
            
            int ret = callback(key, key_len, node->value, node->value_size, user_data);
            free(key);
            
            if (ret != 0) {
                *should_stop = 1;
                return;
            }
        }
    }
    
    for (int i = 0; i < MAX_CHILDREN && !*should_stop; i++) {
        if (node->children[i] && key_len < STREAMDB_MAX_KEY_LEN) {
            key_buf[key_len] = (unsigned char)i;
            collect_all_nodes(node->children[i], key_buf, key_len + 1,
                            callback, user_data, should_stop);
        }
    }
}

StreamDBStatus streamdb_foreach(StreamDB* db, streamdb_foreach_callback callback,
                                 void* user_data) {
    if (!db || !callback) return STREAMDB_INVALID_ARG;
    
    mutex_lock(&db->mutex);
    
    unsigned char* key_buf = (unsigned char*)malloc(STREAMDB_MAX_KEY_LEN);
    if (!key_buf) {
        mutex_unlock(&db->mutex);
        return STREAMDB_NO_MEMORY;
    }
    
    int should_stop = 0;
    collect_all_nodes(db->root, key_buf, 0, callback, user_data, &should_stop);
    
    free(key_buf);
    mutex_unlock(&db->mutex);
    
    return STREAMDB_OK;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

StreamDBStatus streamdb_get_stats(StreamDB* db, StreamDBStats* stats) {
    if (!db || !stats) return STREAMDB_INVALID_ARG;
    
    mutex_lock(&db->mutex);
    
    stats->total_size = db->total_size;
    stats->key_count = db->key_count;
    stats->node_count = db->node_count;
    stats->is_dirty = db->dirty;
    stats->is_file_backend = db->is_file_backend;
    
    mutex_unlock(&db->mutex);
    
    return STREAMDB_OK;
}
