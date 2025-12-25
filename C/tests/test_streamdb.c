/*
 * StreamDB Test Suite
 * 
 * Copyright (C) 2025 DeMoD LLC
 * Licensed under LGPL-2.1
 */

#include "streamdb.h"
#include "streamdb_wrapper.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Test utilities */
#define TEST(name) static int test_##name(void)
#define RUN_TEST(name) do { \
    printf("  %-50s ", #name); \
    fflush(stdout); \
    if (test_##name() == 0) { \
        printf("[PASS]\n"); \
        passed++; \
    } else { \
        printf("[FAIL]\n"); \
        failed++; \
    } \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "\n    Assertion failed: %s (line %d)\n", #cond, __LINE__); \
        return 1; \
    } \
} while(0)

#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define ASSERT_NE(a, b) ASSERT((a) != (b))
#define ASSERT_NULL(p) ASSERT((p) == NULL)
#define ASSERT_NOT_NULL(p) ASSERT((p) != NULL)
#define ASSERT_STR_EQ(a, b) ASSERT(strcmp((a), (b)) == 0)

/* ============================================================================
 * Basic Functionality Tests
 * ============================================================================ */

TEST(init_memory_only) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    streamdb_free(db);
    return 0;
}

TEST(init_with_file) {
    const char* path = "/tmp/streamdb_test_init.dat";
    StreamDB* db = streamdb_init(path, 1000);
    ASSERT_NOT_NULL(db);
    streamdb_free(db);
    remove(path);
    return 0;
}

TEST(insert_and_get) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    const char* key = "test_key";
    const char* value = "test_value";
    
    StreamDBStatus status = streamdb_insert(db, 
        (const unsigned char*)key, strlen(key),
        value, strlen(value) + 1);
    ASSERT_EQ(status, STREAMDB_OK);
    
    size_t size;
    char* result = (char*)streamdb_get(db, 
        (const unsigned char*)key, strlen(key), &size);
    ASSERT_NOT_NULL(result);
    ASSERT_STR_EQ(result, value);
    ASSERT_EQ(size, strlen(value) + 1);
    
    free(result);
    streamdb_free(db);
    return 0;
}

TEST(insert_update) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    const char* key = "key";
    
    streamdb_insert(db, (const unsigned char*)key, 3, "value1", 7);
    streamdb_insert(db, (const unsigned char*)key, 3, "value2", 7);
    
    size_t size;
    char* result = (char*)streamdb_get(db, (const unsigned char*)key, 3, &size);
    ASSERT_NOT_NULL(result);
    ASSERT_STR_EQ(result, "value2");
    
    free(result);
    streamdb_free(db);
    return 0;
}

TEST(delete_key) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    const char* key = "delete_me";
    streamdb_insert(db, (const unsigned char*)key, strlen(key), "value", 6);
    
    StreamDBStatus status = streamdb_delete(db, 
        (const unsigned char*)key, strlen(key));
    ASSERT_EQ(status, STREAMDB_OK);
    
    size_t size;
    void* result = streamdb_get(db, (const unsigned char*)key, strlen(key), &size);
    ASSERT_NULL(result);
    
    streamdb_free(db);
    return 0;
}

TEST(delete_nonexistent) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    StreamDBStatus status = streamdb_delete(db, 
        (const unsigned char*)"nonexistent", 11);
    ASSERT_EQ(status, STREAMDB_NOT_FOUND);
    
    streamdb_free(db);
    return 0;
}

TEST(exists) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    const char* key = "exists_key";
    streamdb_insert(db, (const unsigned char*)key, strlen(key), "v", 2);
    
    ASSERT_EQ(streamdb_exists(db, (const unsigned char*)key, strlen(key)), 1);
    ASSERT_EQ(streamdb_exists(db, (const unsigned char*)"nope", 4), 0);
    
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Binary Key Tests
 * ============================================================================ */

TEST(binary_keys) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    /* Key with null bytes */
    unsigned char key[] = {0x00, 0x01, 0x02, 0x00, 0xFF};
    const char* value = "binary_value";
    
    StreamDBStatus status = streamdb_insert(db, key, 5, value, strlen(value) + 1);
    ASSERT_EQ(status, STREAMDB_OK);
    
    size_t size;
    char* result = (char*)streamdb_get(db, key, 5, &size);
    ASSERT_NOT_NULL(result);
    ASSERT_STR_EQ(result, value);
    
    free(result);
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Suffix Search Tests
 * ============================================================================ */

TEST(suffix_search_basic) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    streamdb_insert(db, (const unsigned char*)"cat", 3, "v1", 3);
    streamdb_insert(db, (const unsigned char*)"car", 3, "v2", 3);
    streamdb_insert(db, (const unsigned char*)"cart", 4, "v3", 3);
    streamdb_insert(db, (const unsigned char*)"bar", 3, "v4", 3);
    
    /* Search for keys ending in "ar" */
    StreamDBResult* results = streamdb_suffix_search(db, 
        (const unsigned char*)"ar", 2);
    
    int count = 0;
    for (StreamDBResult* r = results; r; r = r->next) {
        count++;
        /* Should be "car" or "bar" */
        ASSERT(r->key_len == 3);
    }
    ASSERT_EQ(count, 2);
    
    streamdb_free_results(results);
    streamdb_free(db);
    return 0;
}

TEST(suffix_search_no_match) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    streamdb_insert(db, (const unsigned char*)"hello", 5, "world", 6);
    
    StreamDBResult* results = streamdb_suffix_search(db, 
        (const unsigned char*)"xyz", 3);
    ASSERT_NULL(results);
    
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Persistence Tests
 * ============================================================================ */

TEST(persistence_basic) {
    const char* path = "/tmp/streamdb_test_persist.dat";
    remove(path);
    
    /* Write data */
    {
        StreamDB* db = streamdb_init(path, 0);
        ASSERT_NOT_NULL(db);
        
        streamdb_insert(db, (const unsigned char*)"key1", 4, "value1", 7);
        streamdb_insert(db, (const unsigned char*)"key2", 4, "value2", 7);
        
        StreamDBStatus status = streamdb_flush(db);
        ASSERT_EQ(status, STREAMDB_OK);
        
        streamdb_free(db);
    }
    
    /* Read data back */
    {
        StreamDB* db = streamdb_init(path, 0);
        ASSERT_NOT_NULL(db);
        
        size_t size;
        char* v1 = (char*)streamdb_get(db, (const unsigned char*)"key1", 4, &size);
        ASSERT_NOT_NULL(v1);
        ASSERT_STR_EQ(v1, "value1");
        free(v1);
        
        char* v2 = (char*)streamdb_get(db, (const unsigned char*)"key2", 4, &size);
        ASSERT_NOT_NULL(v2);
        ASSERT_STR_EQ(v2, "value2");
        free(v2);
        
        streamdb_free(db);
    }
    
    remove(path);
    return 0;
}

TEST(flush_memory_only) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    StreamDBStatus status = streamdb_flush(db);
    ASSERT_EQ(status, STREAMDB_NOT_SUPPORTED);
    
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Statistics Tests
 * ============================================================================ */

TEST(stats) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    streamdb_insert(db, (const unsigned char*)"k1", 2, "val1", 5);
    streamdb_insert(db, (const unsigned char*)"k2", 2, "val22", 6);
    
    StreamDBStats stats;
    StreamDBStatus status = streamdb_get_stats(db, &stats);
    ASSERT_EQ(status, STREAMDB_OK);
    ASSERT_EQ(stats.key_count, 2);
    ASSERT_EQ(stats.total_size, 11);
    ASSERT_EQ(stats.is_dirty, 1);
    ASSERT_EQ(stats.is_file_backend, 0);
    
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Foreach Tests
 * ============================================================================ */

static int foreach_count_callback(const unsigned char* key, size_t key_len,
                                   const void* value, size_t value_size,
                                   void* user_data) {
    (void)key; (void)key_len; (void)value; (void)value_size;
    int* count = (int*)user_data;
    (*count)++;
    return 0;
}

static int foreach_stop_callback(const unsigned char* key, size_t key_len,
                                  const void* value, size_t value_size,
                                  void* user_data) {
    (void)key; (void)key_len; (void)value; (void)value_size;
    int* count = (int*)user_data;
    (*count)++;
    return (*count >= 2) ? 1 : 0;  /* Stop after 2 */
}

TEST(foreach) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    streamdb_insert(db, (const unsigned char*)"a", 1, "1", 2);
    streamdb_insert(db, (const unsigned char*)"b", 1, "2", 2);
    streamdb_insert(db, (const unsigned char*)"c", 1, "3", 2);
    
    int count = 0;
    StreamDBStatus status = streamdb_foreach(db, foreach_count_callback, &count);
    ASSERT_EQ(status, STREAMDB_OK);
    ASSERT_EQ(count, 3);
    
    streamdb_free(db);
    return 0;
}

TEST(foreach_early_stop) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    streamdb_insert(db, (const unsigned char*)"a", 1, "1", 2);
    streamdb_insert(db, (const unsigned char*)"b", 1, "2", 2);
    streamdb_insert(db, (const unsigned char*)"c", 1, "3", 2);
    
    int count = 0;
    StreamDBStatus status = streamdb_foreach(db, foreach_stop_callback, &count);
    ASSERT_EQ(status, STREAMDB_OK);
    ASSERT_EQ(count, 2);
    
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Edge Case Tests
 * ============================================================================ */

TEST(null_arguments) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    /* Null key */
    ASSERT_EQ(streamdb_insert(db, NULL, 5, "val", 4), STREAMDB_INVALID_ARG);
    
    /* Zero key length */
    ASSERT_EQ(streamdb_insert(db, (const unsigned char*)"k", 0, "v", 2), 
              STREAMDB_INVALID_ARG);
    
    /* Null value */
    ASSERT_EQ(streamdb_insert(db, (const unsigned char*)"k", 1, NULL, 5), 
              STREAMDB_INVALID_ARG);
    
    /* Zero value size */
    ASSERT_EQ(streamdb_insert(db, (const unsigned char*)"k", 1, "v", 0), 
              STREAMDB_INVALID_ARG);
    
    streamdb_free(db);
    return 0;
}

TEST(max_key_length) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    /* Create max-length key */
    unsigned char key[STREAMDB_MAX_KEY_LEN];
    memset(key, 'x', STREAMDB_MAX_KEY_LEN);
    
    StreamDBStatus status = streamdb_insert(db, key, STREAMDB_MAX_KEY_LEN, "v", 2);
    ASSERT_EQ(status, STREAMDB_OK);
    
    /* Exceed max length */
    status = streamdb_insert(db, key, STREAMDB_MAX_KEY_LEN + 1, "v", 2);
    ASSERT_EQ(status, STREAMDB_INVALID_ARG);
    
    streamdb_free(db);
    return 0;
}

TEST(delete_all_keys) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    streamdb_insert(db, (const unsigned char*)"a", 1, "1", 2);
    streamdb_insert(db, (const unsigned char*)"b", 1, "2", 2);
    
    streamdb_delete(db, (const unsigned char*)"a", 1);
    streamdb_delete(db, (const unsigned char*)"b", 1);
    
    /* Database should still be usable */
    StreamDBStatus status = streamdb_insert(db, (const unsigned char*)"c", 1, "3", 2);
    ASSERT_EQ(status, STREAMDB_OK);
    
    streamdb_free(db);
    return 0;
}

/* ============================================================================
 * Wrapper Tests
 * ============================================================================ */

TEST(wrapper_write_document) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    char guid[37];
    StreamDBStatus status = streamdb_write_document(db, "/docs/test.txt",
                                                     "Hello, World!", 14, guid);
    ASSERT_EQ(status, STREAMDB_OK);
    ASSERT(strlen(guid) == 36);  /* UUID format */
    
    size_t size;
    void* data = streamdb_read_document(db, "/docs/test.txt", &size);
    ASSERT_NOT_NULL(data);
    ASSERT_EQ(size, 14);
    ASSERT_STR_EQ((char*)data, "Hello, World!");
    
    free(data);
    streamdb_free(db);
    return 0;
}

TEST(wrapper_search) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    char guid[37];
    streamdb_write_document(db, "/docs/test.txt", "a", 2, guid);
    streamdb_write_document(db, "/docs/other.txt", "b", 2, guid);
    streamdb_write_document(db, "/images/photo.jpg", "c", 2, guid);
    
    StreamDBSearchResults* results = streamdb_search(db, ".txt");
    ASSERT_NOT_NULL(results);
    ASSERT_EQ(results->count, 2);
    
    streamdb_free_search_results(results);
    streamdb_free(db);
    return 0;
}

TEST(wrapper_batch_write) {
    StreamDB* db = streamdb_init(NULL, 0);
    ASSERT_NOT_NULL(db);
    
    StreamDBBatchEntry entries[] = {
        {"/batch/1", "data1", 6},
        {"/batch/2", "data2", 6},
        {"/batch/3", "data3", 6},
    };
    
    StreamDBStatus status = streamdb_batch_write(db, entries, 3);
    ASSERT_EQ(status, STREAMDB_OK);
    
    ASSERT_EQ(streamdb_exists(db, (const unsigned char*)"/batch/1", 8), 1);
    ASSERT_EQ(streamdb_exists(db, (const unsigned char*)"/batch/2", 8), 1);
    ASSERT_EQ(streamdb_exists(db, (const unsigned char*)"/batch/3", 8), 1);
    
    streamdb_free(db);
    return 0;
}

TEST(wrapper_uuid_generation) {
    char uuid1[37], uuid2[37];
    
    streamdb_generate_uuid(uuid1);
    streamdb_generate_uuid(uuid2);
    
    /* Check format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx */
    ASSERT(strlen(uuid1) == 36);
    ASSERT(uuid1[8] == '-');
    ASSERT(uuid1[13] == '-');
    ASSERT(uuid1[14] == '4');  /* Version 4 */
    ASSERT(uuid1[18] == '-');
    ASSERT(uuid1[23] == '-');
    
    /* UUIDs should be unique */
    ASSERT(strcmp(uuid1, uuid2) != 0);
    
    return 0;
}

/* ============================================================================
 * Utility Tests
 * ============================================================================ */

TEST(strerror) {
    ASSERT(streamdb_strerror(STREAMDB_OK) != NULL);
    ASSERT(streamdb_strerror(STREAMDB_ERROR) != NULL);
    ASSERT(streamdb_strerror(STREAMDB_NOT_FOUND) != NULL);
    ASSERT(streamdb_strerror(STREAMDB_INVALID_ARG) != NULL);
    ASSERT(streamdb_strerror(STREAMDB_NO_MEMORY) != NULL);
    ASSERT(streamdb_strerror(STREAMDB_IO_ERROR) != NULL);
    ASSERT(streamdb_strerror(STREAMDB_NOT_SUPPORTED) != NULL);
    ASSERT(streamdb_strerror(-999) != NULL);  /* Unknown error */
    return 0;
}

TEST(version) {
    const char* version = streamdb_version();
    ASSERT_NOT_NULL(version);
    ASSERT(strlen(version) > 0);
    return 0;
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("\n");
    printf("StreamDB Test Suite v%s\n", streamdb_version());
    printf("============================================================\n\n");
    
    int passed = 0;
    int failed = 0;
    
    printf("Basic Functionality:\n");
    RUN_TEST(init_memory_only);
    RUN_TEST(init_with_file);
    RUN_TEST(insert_and_get);
    RUN_TEST(insert_update);
    RUN_TEST(delete_key);
    RUN_TEST(delete_nonexistent);
    RUN_TEST(exists);
    
    printf("\nBinary Keys:\n");
    RUN_TEST(binary_keys);
    
    printf("\nSuffix Search:\n");
    RUN_TEST(suffix_search_basic);
    RUN_TEST(suffix_search_no_match);
    
    printf("\nPersistence:\n");
    RUN_TEST(persistence_basic);
    RUN_TEST(flush_memory_only);
    
    printf("\nStatistics:\n");
    RUN_TEST(stats);
    
    printf("\nIteration:\n");
    RUN_TEST(foreach);
    RUN_TEST(foreach_early_stop);
    
    printf("\nEdge Cases:\n");
    RUN_TEST(null_arguments);
    RUN_TEST(max_key_length);
    RUN_TEST(delete_all_keys);
    
    printf("\nWrapper Functions:\n");
    RUN_TEST(wrapper_write_document);
    RUN_TEST(wrapper_search);
    RUN_TEST(wrapper_batch_write);
    RUN_TEST(wrapper_uuid_generation);
    
    printf("\nUtilities:\n");
    RUN_TEST(strerror);
    RUN_TEST(version);
    
    printf("\n============================================================\n");
    printf("Results: %d passed, %d failed\n", passed, failed);
    printf("============================================================\n\n");
    
    return failed > 0 ? 1 : 0;
}
