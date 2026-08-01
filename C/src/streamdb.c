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

/*
 * On-disk format v3 — byte-compatible with the Rust implementation
 * (../Rust, crate "streamdb" v2.x persistence format):
 *
 *   [0..128)     header slot 0 (zeroed unless a commit landed there)
 *   [128..256)   header slot 1
 *   [256..)      document region, then appended trie/index commit blobs
 *
 * Header slot (128 bytes, 76 used, little-endian throughout):
 *   0..4   magic "STDB"        4..8   format version u32 (=3)
 *   8..16  commit seq u64      16..24 trie offset u64
 *   24..32 trie len u64        32..36 trie CRC32 u32
 *   40..48 index offset u64    48..56 index len u64
 *   56..60 index CRC32 u32     64..72 data end u64
 *   72..76 CRC32 of bytes 0..72
 *
 * Document record: size u32 LE + CRC32(payload) u32 LE + payload.
 * Document index blob: u64 count + per entry (16-byte UUID, u64 payload
 * offset, u32 size, u32 CRC32), entries sorted by UUID byte order.
 *
 * Trie blob (bincode encoding of the Rust persistent trie):
 *   Trie    := OrdMap children + Option<Uuid> value + u64 count
 *   OrdMap  := u64 len + (u8 key, Trie child)* in ascending byte order
 *   Option  := u8 tag; 1 => u64 len (=16) + 16 UUID bytes
 *
 * Durability protocol: document bytes are appended; flush() appends the
 * trie and index blobs past every document, fdatasync()s, and only then
 * writes the alternating header slot, followed by fsync(). A commit that
 * validates (magic + version + CRC + bounds) therefore always describes a
 * complete snapshot; a crash loses at most the unflushed tail.
 *
 * The pre-v3 C format (native-endian recursive dump, no checksums, no
 * fsync) is gone: those files are rejected on open, as are Rust files with
 * an unrecognised version. Old databases must be rebuilt.
 */

/* Feature test macros - must be defined before any includes */
#if !defined(_WIN32) && !defined(_WIN64)
    #define _POSIX_C_SOURCE 200809L
    #define _DEFAULT_SOURCE
    #define _BSD_SOURCE
    #define _FILE_OFFSET_BITS 64
#endif

#include "streamdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

/* ============================================================================
 * Platform Abstraction Layer
 * ============================================================================ */

#if defined(_WIN32) || defined(_WIN64)
    #define STREAMDB_WINDOWS 1
    #include <windows.h>
    #include <process.h>
    #include <io.h>

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
    #include <fcntl.h>
    #include <sys/time.h>
    #include <sys/file.h>

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

/* --- File-position / sync / locking wrappers ------------------------------- */

static int db_seek(FILE* fp, uint64_t offset) {
#if defined(STREAMDB_WINDOWS)
    return _fseeki64(fp, (long long)offset, SEEK_SET);
#else
    return fseeko(fp, (off_t)offset, SEEK_SET);
#endif
}

static uint64_t db_file_size(FILE* fp) {
#if defined(STREAMDB_WINDOWS)
    if (_fseeki64(fp, 0, SEEK_END) != 0) return 0;
    return (uint64_t)_ftelli64(fp);
#else
    if (fseeko(fp, 0, SEEK_END) != 0) return 0;
    return (uint64_t)ftello(fp);
#endif
}

/* Data must be durable BEFORE the header referencing it (fdatasync). */
static void db_sync_data(FILE* fp) {
#if defined(STREAMDB_POSIX)
    fdatasync(fileno(fp));
#elif defined(STREAMDB_WINDOWS)
    _commit(_fileno(fp));
#else
    fflush(fp);
#endif
}

static void db_sync_all(FILE* fp) {
#if defined(STREAMDB_POSIX)
    fsync(fileno(fp));
#elif defined(STREAMDB_WINDOWS)
    _commit(_fileno(fp));
#else
    fflush(fp);
#endif
}

/* Make a rename() durable: fsync the directory containing the file. */
static void db_sync_parent_dir(const char* path) {
#if defined(STREAMDB_POSIX)
    char* copy = (char*)malloc(strlen(path) + 1);
    if (!copy) return;
    strcpy(copy, path);
    char* slash = strrchr(copy, '/');
    const char* dir = ".";
    if (slash) {
        if (slash == copy) slash[1] = '\0';
        else *slash = '\0';
        dir = copy;
    }
    int fd = open(dir, O_RDONLY);
    if (fd >= 0) {
        fsync(fd);
        close(fd);
    }
    free(copy);
#else
    (void)path;
#endif
}

/*
 * Exclusive advisory lock: two handles on the same path keep independent
 * next_offsets and would interleave appends, silently corrupting the store.
 * Returns 0 on success. Platforms without flock() get no guard.
 */
static int db_lock_file(FILE* fp) {
#if defined(STREAMDB_POSIX)
    return flock(fileno(fp), LOCK_EX | LOCK_NB);
#else
    (void)fp;
    return 0;
#endif
}

/* ============================================================================
 * Internal Constants
 * ============================================================================ */

#define MAX_CHILDREN 256
#define SERIALIZE_STACK_SIZE 4096

#define STREAMDB_FORMAT_VERSION 3
#define HEADER_SLOT_SIZE 128u
#define HEADER_SLOTS 2u
#define DATA_START (HEADER_SLOT_SIZE * HEADER_SLOTS)
#define HEADER_CRC_COVERED 72u

/* ============================================================================
 * CRC32 (IEEE 802.3 — identical to Rust crc32fast / zlib crc32)
 * ============================================================================ */

static uint32_t crc32_table[256];
static int crc32_table_ready = 0;

static void crc32_init_table(void) {
    /* Table contents are deterministic, so a race between two initialisers
     * writes identical values and is harmless. */
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        crc32_table[i] = c;
    }
    crc32_table_ready = 1;
}

static uint32_t streamdb_crc32(const unsigned char* data, size_t len) {
    if (!crc32_table_ready) crc32_init_table();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        c = crc32_table[(c ^ data[i]) & 0xFFu] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

/* ============================================================================
 * Little-endian helpers (portable, alignment-safe)
 * ============================================================================ */

static void put_u32le(unsigned char* p, uint32_t v) {
    p[0] = (unsigned char)(v & 0xFFu);
    p[1] = (unsigned char)((v >> 8) & 0xFFu);
    p[2] = (unsigned char)((v >> 16) & 0xFFu);
    p[3] = (unsigned char)((v >> 24) & 0xFFu);
}

static void put_u64le(unsigned char* p, uint64_t v) {
    for (int i = 0; i < 8; i++) {
        p[i] = (unsigned char)((v >> (8 * i)) & 0xFFu);
    }
}

static uint32_t get_u32le(const unsigned char* p) {
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

static uint64_t get_u64le(const unsigned char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= ((uint64_t)p[i]) << (8 * i);
    }
    return v;
}

/* ============================================================================
 * Header slot encode/decode (mirrors Rust FileHeader)
 * ============================================================================ */

typedef struct {
    uint64_t seq;
    uint64_t trie_offset;
    uint64_t trie_len;
    uint32_t trie_crc;
    uint64_t index_offset;
    uint64_t index_len;
    uint32_t index_crc;
    uint64_t data_end;
} FileHeaderV3;

static void header_encode(unsigned char buf[HEADER_SLOT_SIZE],
                          const FileHeaderV3* h) {
    memset(buf, 0, HEADER_SLOT_SIZE);
    memcpy(buf, "STDB", 4);
    put_u32le(buf + 4, STREAMDB_FORMAT_VERSION);
    put_u64le(buf + 8, h->seq);
    put_u64le(buf + 16, h->trie_offset);
    put_u64le(buf + 24, h->trie_len);
    put_u32le(buf + 32, h->trie_crc);
    put_u64le(buf + 40, h->index_offset);
    put_u64le(buf + 48, h->index_len);
    put_u32le(buf + 56, h->index_crc);
    put_u64le(buf + 64, h->data_end);
    put_u32le(buf + 72, streamdb_crc32(buf, HEADER_CRC_COVERED));
}

/* Returns 1 if the slot describes a valid commit whose referenced regions
 * lie inside the file, 0 otherwise. A slot can be intact yet point past the
 * end of a crash-truncated file; rejecting it falls back to the older slot,
 * which is the whole point of keeping two. */
static int header_decode(const unsigned char* buf, uint64_t file_len,
                         FileHeaderV3* h) {
    if (memcmp(buf, "STDB", 4) != 0) return 0;
    if (get_u32le(buf + 4) != STREAMDB_FORMAT_VERSION) return 0;
    if (get_u32le(buf + 72) != streamdb_crc32(buf, HEADER_CRC_COVERED)) return 0;

    h->seq = get_u64le(buf + 8);
    h->trie_offset = get_u64le(buf + 16);
    h->trie_len = get_u64le(buf + 24);
    h->trie_crc = get_u32le(buf + 32);
    h->index_offset = get_u64le(buf + 40);
    h->index_len = get_u64le(buf + 48);
    h->index_crc = get_u32le(buf + 56);
    h->data_end = get_u64le(buf + 64);

    if (h->trie_len > file_len || h->trie_offset > file_len - h->trie_len) return 0;
    if (h->index_len > file_len || h->index_offset > file_len - h->index_len) return 0;
    if (h->data_end > file_len) return 0;
    return 1;
}

/* ============================================================================
 * UUID v4
 * ============================================================================ */

static void uuid_v4(unsigned char out[16]) {
    int got = 0;
#if defined(STREAMDB_POSIX)
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        ssize_t n = read(fd, out, 16);
        close(fd);
        got = (n == 16);
    }
#endif
    if (!got) {
        static int seeded = 0;
        if (!seeded) {
            srand((unsigned int)time(NULL) ^ (unsigned int)get_pid());
            seeded = 1;
        }
        for (int i = 0; i < 16; i++) {
            out[i] = (unsigned char)(rand() & 0xFF);
        }
    }
    /* Version 4, variant 10xx */
    out[6] = (out[6] & 0x0F) | 0x40;
    out[8] = (out[8] & 0x3F) | 0x80;
}

/* ============================================================================
 * Internal Data Structures
 * ============================================================================ */

/**
 * Trie node: terminates a key by referencing a document UUID (never by
 * holding the value inline — values live in the document store, matching
 * the Rust model).
 */
typedef struct TrieNode {
    struct TrieNode* children[MAX_CHILDREN];
    unsigned char doc_id[16]; /* valid iff has_value */
    int has_value;
    size_t count;             /* keys in this subtree (serialized) */
} TrieNode;

/** Document index entry. Kept in an array sorted by UUID byte order. */
typedef struct {
    unsigned char id[16];
    uint64_t offset;      /* payload offset in the file (after size+crc) */
    uint32_t size;
    uint32_t crc;
    unsigned char* mem;   /* memory-only mode: payload copy; NULL in file mode */
    int live;             /* scratch flag used by compaction */
} DocEntry;

/**
 * Database structure
 */
struct StreamDB {
    TrieNode* root;
    size_t total_size;    /* sum of live document sizes */
    size_t key_count;
    size_t node_count;

    /* Threading */
    mutex_t mutex;
    condvar_t shutdown_cv;

    /* Persistence */
    char* file_path;
    int is_file_backend;
    FILE* fp;             /* file mode: kept open for the handle's lifetime */
    uint64_t next_offset; /* next append position */
    uint64_t seq;         /* last committed header sequence */

    /* Document index, sorted by id */
    DocEntry* docs;
    size_t doc_count;
    size_t doc_cap;

    int dirty;                /* Protected by mutex */
    int running;              /* Protected by mutex */
    int shutdown_requested;   /* Protected by mutex */
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
static void* auto_flush_thread(void* arg);
static StreamDBStatus internal_flush(StreamDB* db);
static void collect_all_nodes_ctx(StreamDB* db, TrieNode* node, unsigned char* key_buf,
                                  size_t key_len,
                                  streamdb_foreach_callback callback, void* user_data,
                                  int* should_stop);
static DocEntry* docs_find(StreamDB* db, const unsigned char* id);
static void doc_remove(StreamDB* db, const unsigned char* id);

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
    free(node);
    if (db) {
        db->node_count--;
    }
}

/* Iterative node freeing to avoid stack overflow */
static void free_node_recursive(StreamDB* db, TrieNode* node) {
    if (!node) return;

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

        for (int i = 0; i < MAX_CHILDREN; i++) {
            if (current->children[i]) {
                if (stack_top < SERIALIZE_STACK_SIZE) {
                    stack[stack_top++] = current->children[i];
                } else {
                    free_node_recursive(db, current->children[i]);
                }
            }
        }

        free_node(db, current);
    }

    free(stack);
}

/* ============================================================================
 * Document index (sorted array, binary search)
 * ============================================================================ */

static size_t docs_lower_bound(const StreamDB* db, const unsigned char* id) {
    size_t lo = 0, hi = db->doc_count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (memcmp(db->docs[mid].id, id, 16) < 0) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static DocEntry* docs_find(StreamDB* db, const unsigned char* id) {
    size_t i = docs_lower_bound(db, id);
    if (i < db->doc_count && memcmp(db->docs[i].id, id, 16) == 0) {
        return &db->docs[i];
    }
    return NULL;
}

static int docs_grow(StreamDB* db) {
    if (db->doc_count < db->doc_cap) return 0;
    size_t new_cap = db->doc_cap ? db->doc_cap * 2 : 64;
    DocEntry* p = (DocEntry*)realloc(db->docs, new_cap * sizeof(DocEntry));
    if (!p) return -1;
    db->docs = p;
    db->doc_cap = new_cap;
    return 0;
}

/* Insert keeping sorted order. IDs are fresh UUIDs, so duplicates are not
 * expected; an equal ID overwrites defensively. */
static int docs_put(StreamDB* db, const DocEntry* entry) {
    size_t i = docs_lower_bound(db, entry->id);
    if (i < db->doc_count && memcmp(db->docs[i].id, entry->id, 16) == 0) {
        db->docs[i] = *entry;
        return 0;
    }
    if (docs_grow(db) != 0) return -1;
    memmove(&db->docs[i + 1], &db->docs[i], (db->doc_count - i) * sizeof(DocEntry));
    db->docs[i] = *entry;
    db->doc_count++;
    return 0;
}

static void docs_remove_at(StreamDB* db, size_t i) {
    memmove(&db->docs[i], &db->docs[i + 1], (db->doc_count - i - 1) * sizeof(DocEntry));
    db->doc_count--;
}

/* ============================================================================
 * Document store
 * ============================================================================ */

/* Write a document (append in file mode, malloc in memory mode) and index it.
 * Bytes are NOT synced here: durability happens at flush, like the Rust side. */
static StreamDBStatus doc_write(StreamDB* db, const void* data, size_t size,
                                unsigned char id_out[16]) {
    DocEntry e;
    memset(&e, 0, sizeof(e));
    uuid_v4(e.id);
    e.size = (uint32_t)size;
    e.crc = streamdb_crc32((const unsigned char*)data, size);

    if (db->is_file_backend) {
        unsigned char hdr[8];
        put_u32le(hdr, e.size);
        put_u32le(hdr + 4, e.crc);

        e.offset = db->next_offset + 8;
        if (db_seek(db->fp, db->next_offset) != 0) return STREAMDB_IO_ERROR;
        if (fwrite(hdr, 1, 8, db->fp) != 8) return STREAMDB_IO_ERROR;
        if (size > 0 && fwrite(data, 1, size, db->fp) != size) return STREAMDB_IO_ERROR;
        db->next_offset += (uint64_t)size + 8;
    } else {
        e.offset = 0;
        e.mem = (unsigned char*)malloc(size ? size : 1);
        if (!e.mem) return STREAMDB_NO_MEMORY;
        memcpy(e.mem, data, size);
    }

    if (docs_put(db, &e) != 0) {
        free(e.mem);
        return STREAMDB_NO_MEMORY;
    }

    db->total_size += size;
    memcpy(id_out, e.id, 16);
    return STREAMDB_OK;
}

/* Read a document, verifying its CRC32. Returns a caller-owned copy. */
static void* doc_read(StreamDB* db, const unsigned char* id, size_t* size_out) {
    DocEntry* e = docs_find(db, id);
    if (!e) return NULL;

    unsigned char* buf = (unsigned char*)malloc(e->size ? e->size : 1);
    if (!buf) return NULL;

    if (e->mem) {
        memcpy(buf, e->mem, e->size);
    } else {
        if (db_seek(db->fp, e->offset) != 0 ||
            (e->size > 0 && fread(buf, 1, e->size, db->fp) != e->size)) {
            free(buf);
            return NULL;
        }
    }

    if (streamdb_crc32(buf, e->size) != e->crc) {
        /* Corruption detected (checksum mismatch). */
        free(buf);
        return NULL;
    }

    *size_out = e->size;
    return buf;
}

/* Remove a document from the index. In file mode the bytes stay on disk
 * until compaction, exactly like the Rust side. */
static void doc_remove(StreamDB* db, const unsigned char* id) {
    size_t i = docs_lower_bound(db, id);
    if (i < db->doc_count && memcmp(db->docs[i].id, id, 16) == 0) {
        db->total_size -= db->docs[i].size;
        free(db->docs[i].mem);
        docs_remove_at(db, i);
    }
}

/* ============================================================================
 * Trie serialization (bincode-compatible with the Rust persistent trie)
 * ============================================================================ */

typedef struct {
    unsigned char* data;
    size_t len;
    size_t cap;
    int ok;
} OutBuf;

static void out_reserve(OutBuf* o, size_t extra) {
    if (!o->ok) return;
    if (o->len + extra <= o->cap) return;
    size_t new_cap = o->cap ? o->cap * 2 : 4096;
    while (new_cap < o->len + extra) new_cap *= 2;
    unsigned char* p = (unsigned char*)realloc(o->data, new_cap);
    if (!p) {
        o->ok = 0;
        return;
    }
    o->data = p;
    o->cap = new_cap;
}

static void out_bytes(OutBuf* o, const void* p, size_t n) {
    if (!o->ok) return;
    out_reserve(o, n);
    if (!o->ok) return;
    memcpy(o->data + o->len, p, n);
    o->len += n;
}

static void out_u8(OutBuf* o, unsigned char v) {
    out_bytes(o, &v, 1);
}

static void out_u32le(OutBuf* o, uint32_t v) {
    unsigned char b[4];
    put_u32le(b, v);
    out_bytes(o, b, 4);
}

static void out_u64le(OutBuf* o, uint64_t v) {
    unsigned char b[8];
    put_u64le(b, v);
    out_bytes(o, b, 8);
}

/*
 * Trie := OrdMap children + Option<Uuid> value + u64 count
 * OrdMap := u64 len + (u8 key, Trie child)* ascending
 * Recursion depth is bounded by STREAMDB_MAX_KEY_LEN (1024).
 */
static void ser_node(OutBuf* o, const TrieNode* node) {
    uint64_t nchild = 0;
    for (int i = 0; i < MAX_CHILDREN; i++) {
        if (node->children[i]) nchild++;
    }
    out_u64le(o, nchild);
    for (int i = 0; i < MAX_CHILDREN; i++) {
        if (node->children[i]) {
            out_u8(o, (unsigned char)i);
            ser_node(o, node->children[i]);
        }
    }
    if (node->has_value) {
        out_u8(o, 1);
        out_u64le(o, 16);
        out_bytes(o, node->doc_id, 16);
    } else {
        out_u8(o, 0);
    }
    out_u64le(o, (uint64_t)node->count);
}

/* ============================================================================
 * Trie parsing (bounds-checked cursor, count-verified)
 * ============================================================================ */

typedef struct {
    const unsigned char* data;
    size_t len;
    size_t pos;
} Cur;

static const unsigned char* cur_take(Cur* c, size_t n) {
    if (n > c->len || c->pos > c->len - n) return NULL;
    const unsigned char* p = c->data + c->pos;
    c->pos += n;
    return p;
}

static int cur_u64(Cur* c, uint64_t* out) {
    const unsigned char* p = cur_take(c, 8);
    if (!p) return 0;
    *out = get_u64le(p);
    return 1;
}

static TrieNode* parse_node(Cur* c, StreamDB* db, unsigned depth) {
    if (depth > STREAMDB_MAX_KEY_LEN) return NULL;

    uint64_t nchild;
    if (!cur_u64(c, &nchild)) return NULL;
    if (nchild > MAX_CHILDREN) return NULL; /* u8 keys: at most 256 */

    TrieNode* node = create_node(db);
    if (!node) return NULL;

    for (uint64_t i = 0; i < nchild; i++) {
        const unsigned char* byte = cur_take(c, 1);
        if (!byte) goto fail;
        TrieNode* child = parse_node(c, db, depth + 1);
        if (!child) goto fail;
        /* Duplicate byte key in a corrupt file: last wins. */
        if (node->children[*byte]) {
            free_node_recursive(db, node->children[*byte]);
        }
        node->children[*byte] = child;
    }

    {
        const unsigned char* tag = cur_take(c, 1);
        if (!tag) goto fail;
        if (*tag == 1) {
            uint64_t ulen;
            const unsigned char* id;
            if (!cur_u64(c, &ulen)) goto fail;
            if (ulen != 16) goto fail;
            id = cur_take(c, 16);
            if (!id) goto fail;
            memcpy(node->doc_id, id, 16);
            node->has_value = 1;
        } else if (*tag != 0) {
            goto fail;
        }
    }

    {
        uint64_t count;
        uint64_t computed = node->has_value ? 1u : 0u;
        if (!cur_u64(c, &count)) goto fail;
        for (int i = 0; i < MAX_CHILDREN; i++) {
            if (node->children[i]) computed += node->children[i]->count;
        }
        if (computed != count) goto fail; /* inconsistent: corrupt */
        node->count = (size_t)count;
    }

    return node;

fail:
    free_node_recursive(db, node);
    return NULL;
}

/* ============================================================================
 * Persistence: commit / load / initialize
 * ============================================================================ */

/* Serialize the document index blob: u64 count + sorted entries. */
static void serialize_index(OutBuf* o, const DocEntry* docs, size_t n) {
    out_u64le(o, (uint64_t)n);
    for (size_t i = 0; i < n; i++) {
        out_bytes(o, docs[i].id, 16);
        out_u64le(o, docs[i].offset);
        out_u32le(o, docs[i].size);
        out_u32le(o, docs[i].crc);
    }
}

/* Append-then-commit-header, mirroring Rust FileBackend::flush. The caller
 * must hold db->mutex (all call sites do). */
static StreamDBStatus internal_flush(StreamDB* db) {
    if (!db->is_file_backend || !db->fp) {
        return STREAMDB_NOT_SUPPORTED;
    }

    OutBuf trie = {0, 0, 0, 1};
    ser_node(&trie, db->root);
    if (!trie.ok) {
        free(trie.data);
        return STREAMDB_NO_MEMORY;
    }
    uint32_t trie_crc = streamdb_crc32(trie.data, trie.len);

    OutBuf index = {0, 0, 0, 1};
    serialize_index(&index, db->docs, db->doc_count);
    if (!index.ok) {
        free(trie.data);
        free(index.data);
        return STREAMDB_NO_MEMORY;
    }
    uint32_t index_crc = streamdb_crc32(index.data, index.len);

    uint64_t trie_offset = db->next_offset;
    uint64_t index_offset = trie_offset + trie.len;
    uint64_t data_end = index_offset + index.len;

    StreamDBStatus status = STREAMDB_OK;

    if (db_seek(db->fp, trie_offset) != 0 ||
        (trie.len > 0 && fwrite(trie.data, 1, trie.len, db->fp) != trie.len) ||
        (index.len > 0 && fwrite(index.data, 1, index.len, db->fp) != index.len)) {
        status = STREAMDB_IO_ERROR;
        goto done;
    }
    fflush(db->fp);
    /* Ordering is the whole guarantee: data durable BEFORE the header. */
    db_sync_data(db->fp);

    {
        FileHeaderV3 h;
        unsigned char slot[HEADER_SLOT_SIZE];
        h.seq = db->seq + 1;
        h.trie_offset = trie_offset;
        h.trie_len = trie.len;
        h.trie_crc = trie_crc;
        h.index_offset = index_offset;
        h.index_len = index.len;
        h.index_crc = index_crc;
        h.data_end = data_end;
        header_encode(slot, &h);

        if (db_seek(db->fp, (h.seq % HEADER_SLOTS) * HEADER_SLOT_SIZE) != 0 ||
            fwrite(slot, 1, HEADER_SLOT_SIZE, db->fp) != HEADER_SLOT_SIZE) {
            status = STREAMDB_IO_ERROR;
            goto done;
        }
        fflush(db->fp);
        db_sync_all(db->fp);

        db->seq = h.seq;
        db->next_offset = data_end;
    }

done:
    free(trie.data);
    free(index.data);
    return status;
}

/* Fresh file: slot 0 zeroed (invalid magic), slot 1 holds the initial
 * empty commit. */
static int initialize_file(StreamDB* db) {
    unsigned char zeros[HEADER_SLOT_SIZE];
    memset(zeros, 0, sizeof(zeros));

    FileHeaderV3 h;
    memset(&h, 0, sizeof(h));
    h.seq = 1;
    h.data_end = DATA_START;

    unsigned char slot[HEADER_SLOT_SIZE];
    header_encode(slot, &h);

    if (db_seek(db->fp, 0) != 0) return 0;
    if (fwrite(zeros, 1, HEADER_SLOT_SIZE, db->fp) != HEADER_SLOT_SIZE) return 0;
    if (fwrite(slot, 1, HEADER_SLOT_SIZE, db->fp) != HEADER_SLOT_SIZE) return 0;
    fflush(db->fp);
    db_sync_all(db->fp);

    db->seq = 1;
    db->next_offset = DATA_START;
    return 1;
}

/* Load the trie + document index described by one header into the live DB.
 * Returns 1 on success; on failure the DB state is untouched. */
static int load_commit(StreamDB* db, const FileHeaderV3* h) {
    TrieNode* new_root = NULL;
    DocEntry* new_docs = NULL;
    size_t new_doc_count = 0;
    size_t new_total = 0;

    if (h->trie_len == 0) {
        /* Empty database commit. */
        new_root = create_node(db);
        if (!new_root) return 0;
    } else {
        /* Trie blob */
        unsigned char* trie_buf = (unsigned char*)malloc(h->trie_len);
        if (!trie_buf) return 0;
        if (db_seek(db->fp, h->trie_offset) != 0 ||
            fread(trie_buf, 1, h->trie_len, db->fp) != h->trie_len) {
            free(trie_buf);
            return 0;
        }
        if (streamdb_crc32(trie_buf, h->trie_len) != h->trie_crc) {
            free(trie_buf);
            return 0;
        }

        Cur c = {trie_buf, (size_t)h->trie_len, 0};
        new_root = parse_node(&c, db, 0);
        int consumed = (c.pos == (size_t)h->trie_len);
        free(trie_buf);
        if (!new_root || !consumed) {
            if (new_root) free_node_recursive(db, new_root);
            return 0;
        }

        /* Index blob */
        unsigned char* idx_buf = (unsigned char*)malloc(h->index_len ? h->index_len : 1);
        if (!idx_buf) {
            free_node_recursive(db, new_root);
            return 0;
        }
        if (db_seek(db->fp, h->index_offset) != 0 ||
            (h->index_len > 0 && fread(idx_buf, 1, h->index_len, db->fp) != h->index_len)) {
            free(idx_buf);
            free_node_recursive(db, new_root);
            return 0;
        }
        if (streamdb_crc32(idx_buf, h->index_len) != h->index_crc) {
            free(idx_buf);
            free_node_recursive(db, new_root);
            return 0;
        }

        Cur ic = {idx_buf, (size_t)h->index_len, 0};
        uint64_t count = 0;
        int parse_ok = cur_u64(&ic, &count);
        if (parse_ok && count > (h->index_len - 8) / 32) {
            parse_ok = 0; /* entries can't fit: corrupt */
        }
        if (parse_ok && count > 0) {
            new_docs = (DocEntry*)calloc(count, sizeof(DocEntry));
            if (!new_docs) parse_ok = 0;
        }
        for (uint64_t i = 0; parse_ok && i < count; i++) {
            const unsigned char* id = cur_take(&ic, 16);
            const unsigned char* off = cur_take(&ic, 8);
            const unsigned char* sz = cur_take(&ic, 4);
            const unsigned char* cr = cur_take(&ic, 4);
            if (!id || !off || !sz || !cr) {
                parse_ok = 0;
                break;
            }
            memcpy(new_docs[i].id, id, 16);
            new_docs[i].offset = get_u64le(off);
            new_docs[i].size = get_u32le(sz);
            new_docs[i].crc = get_u32le(cr);
            /* File must be sorted by id; corruption otherwise. */
            if (i > 0 && memcmp(new_docs[i - 1].id, id, 16) >= 0) {
                parse_ok = 0;
                break;
            }
            new_total += new_docs[i].size;
        }
        free(idx_buf);
        if (!parse_ok) {
            free(new_docs);
            free_node_recursive(db, new_root);
            return 0;
        }
        new_doc_count = (size_t)count;
    }

    /* Success: swap into the live handle. */
    for (size_t i = 0; i < db->doc_count; i++) {
        free(db->docs[i].mem);
    }
    free(db->docs);
    db->docs = new_docs;
    db->doc_count = new_doc_count;
    db->doc_cap = new_doc_count;

    free_node_recursive(db, db->root);
    db->root = new_root;
    db->key_count = new_root->count;
    db->total_size = new_total;
    db->dirty = 0;
    return 1;
}

/* Read both header slots, pick the newest valid commit, fall back on
 * corruption exactly like the Rust loader. */
static int load_file(StreamDB* db) {
    uint64_t file_len = db_file_size(db->fp);
    if (file_len == 0) {
        return initialize_file(db);
    }

    unsigned char slots[DATA_START];
    size_t readable = file_len < DATA_START ? (size_t)file_len : DATA_START;
    if (db_seek(db->fp, 0) != 0) return 0;
    if (fread(slots, 1, readable, db->fp) != readable) return 0;

    FileHeaderV3 candidates[HEADER_SLOTS];
    int n_candidates = 0;
    for (uint64_t i = 0; i < HEADER_SLOTS; i++) {
        uint64_t start = i * HEADER_SLOT_SIZE;
        if (start + HEADER_SLOT_SIZE > readable) continue;
        FileHeaderV3 h;
        if (header_decode(slots + start, file_len, &h)) {
            /* Insert sorted by seq descending. */
            int j = n_candidates++;
            while (j > 0 && candidates[j - 1].seq < h.seq) {
                candidates[j] = candidates[j - 1];
                j--;
            }
            candidates[j] = h;
        }
    }

    if (n_candidates == 0) {
        fprintf(stderr,
                "StreamDB: no valid header slot in %s (not a v3 StreamDB file, "
                "or both commits damaged)\n", db->file_path);
        return 0;
    }

    for (int i = 0; i < n_candidates; i++) {
        if (load_commit(db, &candidates[i])) {
            db->seq = candidates[i].seq;
            db->next_offset = candidates[i].data_end > DATA_START
                            ? candidates[i].data_end
                            : DATA_START;
            return 1;
        }
        fprintf(stderr,
                "StreamDB: header slot seq=%llu unusable, trying older commit\n",
                (unsigned long long)candidates[i].seq);
    }

    return 0;
}

/* ============================================================================
 * Auto-flush thread
 * ============================================================================ */

static void* auto_flush_thread(void* arg) {
    StreamDB* db = (StreamDB*)arg;

    mutex_lock(&db->mutex);

    while (db->running && !db->shutdown_requested) {
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

    /* Open (or create) and load the database file. */
    if (db->is_file_backend && db->file_path) {
        db->fp = fopen(db->file_path, "r+b");
        if (!db->fp) {
            db->fp = fopen(db->file_path, "w+b");
        }
        if (!db->fp) {
            fprintf(stderr, "StreamDB: cannot open %s\n", db->file_path);
            condvar_destroy(&db->shutdown_cv);
            mutex_destroy(&db->mutex);
            free_node(db, db->root);
            free(db->file_path);
            free(db);
            return NULL;
        }

        /* Exclusive advisory lock: a second opener corrupts the store. */
        if (db_lock_file(db->fp) != 0) {
            fprintf(stderr,
                    "StreamDB: %s is already open in this or another process\n",
                    db->file_path);
            fclose(db->fp);
            condvar_destroy(&db->shutdown_cv);
            mutex_destroy(&db->mutex);
            free_node(db, db->root);
            free(db->file_path);
            free(db);
            return NULL;
        }

        if (!load_file(db)) {
            fclose(db->fp);
            condvar_destroy(&db->shutdown_cv);
            mutex_destroy(&db->mutex);
            free_node_recursive(db, db->root);
            free(db->file_path);
            free(db);
            return NULL;
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

    /* Free document index (memory-mode payloads are owned) */
    for (size_t i = 0; i < db->doc_count; i++) {
        free(db->docs[i].mem);
    }
    free(db->docs);

    if (db->fp) {
        fclose(db->fp); /* releases the advisory lock */
    }

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
    if (value_size > 0xFFFFFFFFu) {
        return STREAMDB_INVALID_ARG; /* format: u32 size field */
    }

    mutex_lock(&db->mutex);

    /* Traverse/create nodes in reverse order */
    TrieNode* current = db->root;
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

    int is_new_key = !current->has_value;
    unsigned char old_id[16];
    if (!is_new_key) {
        memcpy(old_id, current->doc_id, 16);
    }

    /* Write the new document first; on failure the old state is intact. */
    unsigned char new_id[16];
    StreamDBStatus status = doc_write(db, value, value_size, new_id);
    if (status != STREAMDB_OK) {
        mutex_unlock(&db->mutex);
        return status;
    }

    /* Reclaim the superseded document so updates don't leak storage.
     * Readers that resolved the old ID concurrently may see a transient
     * miss — same race class as delete(). */
    if (!is_new_key) {
        doc_remove(db, old_id);
    }

    memcpy(current->doc_id, new_id, 16);
    current->has_value = 1;

    if (is_new_key) {
        db->key_count++;
        /* Bump subtree counts along the path. */
        db->root->count++;
        TrieNode* n = db->root;
        for (int i = (int)key_len - 1; i >= 0; i--) {
            n = n->children[key[i]];
            n->count++;
        }
    }

    db->dirty = 1;

    mutex_unlock(&db->mutex);
    return STREAMDB_OK;
}

/* Internal find without mutex (for use within locked sections) */
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

    return current->has_value ? current : NULL;
}

void* streamdb_get(StreamDB* db, const unsigned char* key, size_t key_len,
                   size_t* value_size) {
    if (!value_size) return NULL;
    *value_size = 0;

    if (!db || !key || key_len == 0) return NULL;
    mutex_lock(&db->mutex);

    TrieNode* node = internal_find(db, key, key_len);
    void* result = NULL;

    if (node) {
        result = doc_read(db, node->doc_id, value_size);
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

/* Recursive helper for delete - never deletes the root. Counts are
 * recomputed bottom-up, so pruning and count stay consistent. */
static TrieNode* remove_helper(StreamDB* db, TrieNode* node, const unsigned char* key,
                                size_t key_len, size_t index, int* removed, int is_root) {
    if (!node) return NULL;

    if (index == key_len) {
        if (node->has_value) {
            doc_remove(db, node->doc_id);
            node->has_value = 0;
            db->key_count--;
            *removed = 1;
        }
    } else {
        unsigned char c = key[key_len - 1 - index];
        node->children[c] = remove_helper(db, node->children[c], key, key_len,
                                          index + 1, removed, 0);
    }

    size_t cnt = node->has_value ? 1 : 0;
    for (int i = 0; i < MAX_CHILDREN; i++) {
        if (node->children[i]) cnt += node->children[i]->count;
    }
    node->count = cnt;

    /* Never delete the root node */
    if (cnt == 0 && !is_root) {
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

    int removed = 0;
    db->root = remove_helper(db, db->root, key, key_len, 0, &removed, 1);

    /* Ensure root is never NULL */
    if (!db->root) {
        db->root = create_node(db);
    }

    StreamDBStatus status;
    if (removed) {
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
static void collect_results(StreamDB* db, TrieNode* node, unsigned char* extension,
                            size_t ext_len,
                            const unsigned char* rev_suffix, size_t suffix_len,
                            StreamDBResult** results, int* error) {
    if (!node || *error) return;

    if (node->has_value) {
        size_t full_len = suffix_len + ext_len;
        if (full_len <= STREAMDB_MAX_KEY_LEN) {
            size_t vsize = 0;
            void* vcopy = doc_read(db, node->doc_id, &vsize);
            if (!vcopy) {
                *error = 1;
                return;
            }

            /* Build the full reversed key */
            unsigned char* full_rev = (unsigned char*)malloc(full_len);
            if (!full_rev) {
                free(vcopy);
                *error = 1;
                return;
            }
            memcpy(full_rev, rev_suffix, suffix_len);
            memcpy(full_rev + suffix_len, extension, ext_len);

            /* Reverse to get original key */
            unsigned char* key = (unsigned char*)malloc(full_len);
            if (!key) {
                free(full_rev);
                free(vcopy);
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
                free(vcopy);
                *error = 1;
                return;
            }

            res->key = key;
            res->key_len = full_len;
            res->value = vcopy;
            res->value_size = vsize;
            res->next = *results;
            *results = res;
        }
    }

    for (int i = 0; i < MAX_CHILDREN; i++) {
        if (node->children[i]) {
            if (ext_len >= STREAMDB_MAX_KEY_LEN - suffix_len) continue;
            extension[ext_len] = (unsigned char)i;
            collect_results(db, node->children[i], extension, ext_len + 1,
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
        collect_results(db, current, extension, 0, rev_suffix, suffix_len, &results, &error);
        free(extension);
    } else {
        error = 1;
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

static void collect_all_nodes_ctx(StreamDB* db, TrieNode* node, unsigned char* key_buf,
                                  size_t key_len,
                                  streamdb_foreach_callback callback, void* user_data,
                                  int* should_stop) {
    if (!node || *should_stop) return;

    if (node->has_value) {
        size_t vsize = 0;
        void* vcopy = doc_read(db, node->doc_id, &vsize);
        if (vcopy) {
            /* Reverse the key buffer to get original key */
            unsigned char* key = (unsigned char*)malloc(key_len);
            if (key) {
                for (size_t i = 0; i < key_len; i++) {
                    key[i] = key_buf[key_len - 1 - i];
                }

                int ret = callback(key, key_len, vcopy, vsize, user_data);
                free(key);

                if (ret != 0) {
                    free(vcopy);
                    *should_stop = 1;
                    return;
                }
            }
            free(vcopy);
        }
    }

    for (int i = 0; i < MAX_CHILDREN && !*should_stop; i++) {
        if (node->children[i] && key_len < STREAMDB_MAX_KEY_LEN) {
            key_buf[key_len] = (unsigned char)i;
            collect_all_nodes_ctx(db, node->children[i], key_buf, key_len + 1,
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
    collect_all_nodes_ctx(db, db->root, key_buf, 0, callback, user_data, &should_stop);

    free(key_buf);
    mutex_unlock(&db->mutex);

    return STREAMDB_OK;
}

/* ============================================================================
 * Persistence Operations
 * ============================================================================ */

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

/* Mark every document referenced by the trie as live (compaction GC). */
static void mark_live(StreamDB* db, TrieNode* node) {
    if (!node) return;
    if (node->has_value) {
        DocEntry* e = docs_find(db, node->doc_id);
        if (e) e->live = 1;
    }
    for (int i = 0; i < MAX_CHILDREN; i++) {
        mark_live(db, node->children[i]);
    }
}

StreamDBStatus streamdb_compact(StreamDB* db) {
    if (!db) return STREAMDB_INVALID_ARG;

    mutex_lock(&db->mutex);

    if (!db->is_file_backend || !db->fp) {
        mutex_unlock(&db->mutex);
        return STREAMDB_NOT_SUPPORTED;
    }

    StreamDBStatus status = STREAMDB_OK;
    DocEntry* new_docs = NULL;
    size_t new_count = 0;
    uint64_t new_data_end = DATA_START;

    /* GC: only documents referenced by the current trie are copied; orphans
     * (superseded updates whose cleanup failed, failed deletes) are dropped. */
    for (size_t i = 0; i < db->doc_count; i++) {
        db->docs[i].live = 0;
    }
    mark_live(db, db->root);

    /* Temp file sibling */
    size_t tmp_len = strlen(db->file_path) + 9;
    char* tmp_path = (char*)malloc(tmp_len);
    if (!tmp_path) {
        mutex_unlock(&db->mutex);
        return STREAMDB_NO_MEMORY;
    }
    snprintf(tmp_path, tmp_len, "%s.compact", db->file_path);

    FILE* out = fopen(tmp_path, "w+b");
    if (!out) {
        free(tmp_path);
        mutex_unlock(&db->mutex);
        return STREAMDB_IO_ERROR;
    }

    /* Reserve both header slots */
    {
        unsigned char zeros[DATA_START];
        memset(zeros, 0, sizeof(zeros));
        if (fwrite(zeros, 1, DATA_START, out) != DATA_START) {
            status = STREAMDB_IO_ERROR;
        }
    }

    if (status == STREAMDB_OK) {
        new_docs = (DocEntry*)calloc(db->doc_count ? db->doc_count : 1, sizeof(DocEntry));
        if (!new_docs) status = STREAMDB_NO_MEMORY;
    }

    /* Copy live documents */
    uint64_t offset = DATA_START;
    for (size_t i = 0; status == STREAMDB_OK && i < db->doc_count; i++) {
        if (!db->docs[i].live) continue;

        unsigned char* payload = (unsigned char*)malloc(db->docs[i].size);
        if (!payload) {
            status = STREAMDB_NO_MEMORY;
            break;
        }
        if (db_seek(db->fp, db->docs[i].offset) != 0 ||
            (db->docs[i].size > 0 &&
             fread(payload, 1, db->docs[i].size, db->fp) != db->docs[i].size)) {
            free(payload);
            status = STREAMDB_IO_ERROR;
            break;
        }
        if (streamdb_crc32(payload, db->docs[i].size) != db->docs[i].crc) {
            /* Corruption detected during compaction: abort, original intact. */
            free(payload);
            status = STREAMDB_IO_ERROR;
            break;
        }

        unsigned char hdr[8];
        put_u32le(hdr, db->docs[i].size);
        put_u32le(hdr + 4, db->docs[i].crc);
        if (fwrite(hdr, 1, 8, out) != 8 ||
            (db->docs[i].size > 0 &&
             fwrite(payload, 1, db->docs[i].size, out) != db->docs[i].size)) {
            free(payload);
            status = STREAMDB_IO_ERROR;
            break;
        }
        free(payload);

        new_docs[new_count] = db->docs[i];
        new_docs[new_count].mem = NULL;
        new_docs[new_count].offset = offset + 8;
        new_count++;
        offset += (uint64_t)db->docs[i].size + 8;
    }

    /* One fresh commit at the head of the new file */
    if (status == STREAMDB_OK) {
        OutBuf trie = {0, 0, 0, 1};
        ser_node(&trie, db->root);
        if (!trie.ok) status = STREAMDB_NO_MEMORY;

        OutBuf index = {0, 0, 0, 1};
        if (status == STREAMDB_OK) {
            serialize_index(&index, new_docs, new_count);
            if (!index.ok) status = STREAMDB_NO_MEMORY;
        }

        if (status == STREAMDB_OK) {
            uint64_t trie_offset = offset;
            uint64_t index_offset = trie_offset + trie.len;
            new_data_end = index_offset + index.len;

            if (db_seek(out, trie_offset) != 0 ||
                (trie.len > 0 && fwrite(trie.data, 1, trie.len, out) != trie.len) ||
                (index.len > 0 && fwrite(index.data, 1, index.len, out) != index.len)) {
                status = STREAMDB_IO_ERROR;
            } else {
                FileHeaderV3 h;
                unsigned char slot[HEADER_SLOT_SIZE];
                h.seq = 1;
                h.trie_offset = trie_offset;
                h.trie_len = trie.len;
                h.trie_crc = streamdb_crc32(trie.data, trie.len);
                h.index_offset = index_offset;
                h.index_len = index.len;
                h.index_crc = streamdb_crc32(index.data, index.len);
                h.data_end = new_data_end;
                header_encode(slot, &h);

                if (db_seek(out, (h.seq % HEADER_SLOTS) * HEADER_SLOT_SIZE) != 0 ||
                    fwrite(slot, 1, HEADER_SLOT_SIZE, out) != HEADER_SLOT_SIZE) {
                    status = STREAMDB_IO_ERROR;
                }
            }
        }

        free(trie.data);
        free(index.data);
    }

    if (status == STREAMDB_OK) {
        fflush(out);
        db_sync_all(out);
    }
    fclose(out);

    if (status == STREAMDB_OK) {
        if (rename(tmp_path, db->file_path) != 0) {
            status = STREAMDB_IO_ERROR;
        } else {
            /* Make the rename itself durable. */
            db_sync_parent_dir(db->file_path);
        }
    }

    if (status == STREAMDB_OK) {
        /* Reopen the new file and re-acquire the advisory lock. */
        fclose(db->fp);
        db->fp = fopen(db->file_path, "r+b");
        if (!db->fp) {
            status = STREAMDB_IO_ERROR;
        } else if (db_lock_file(db->fp) != 0) {
            fclose(db->fp);
            db->fp = NULL;
            status = STREAMDB_IO_ERROR;
        }
    }

    if (status == STREAMDB_OK) {
        /* Swap the index. File-mode docs have no owned payloads. */
        size_t new_total = 0;
        for (size_t i = 0; i < new_count; i++) {
            new_total += new_docs[i].size;
        }
        free(db->docs);
        db->docs = new_docs;
        db->doc_count = new_count;
        db->doc_cap = new_count;
        db->total_size = new_total;
        db->seq = 1;
        db->next_offset = new_data_end;
        /* The compact commit covers the entire current trie. */
        db->dirty = 0;
    } else {
        remove(tmp_path);
        free(new_docs);
    }

    free(tmp_path);
    mutex_unlock(&db->mutex);
    return status;
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
