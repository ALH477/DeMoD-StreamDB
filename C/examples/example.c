/*
 * StreamDB Example Usage
 * 
 * Copyright (C) 2025 DeMoD LLC
 * Licensed under LGPL-2.1
 * 
 * Compile: gcc -o example example.c -I../include -L../lib -lstreamdb -lpthread
 */

#include "streamdb.h"
#include "streamdb_wrapper.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Callback for foreach example */
static int print_entry(const unsigned char* key, size_t key_len,
                       const void* value, size_t value_size,
                       void* user_data) {
    (void)user_data;
    printf("  Key: %.*s, Value: %.*s\n", 
           (int)key_len, (char*)key,
           (int)value_size - 1, (char*)value);
    return 0;  /* Continue iteration */
}

int main(void) {
    printf("StreamDB Example v%s\n", streamdb_version());
    printf("================================================\n\n");
    
    /* ================================================================
     * Basic Usage
     * ================================================================ */
    printf("1. Basic Usage\n");
    printf("--------------\n");
    
    /* Initialize with file persistence */
    StreamDB* db = streamdb_init("example.dat", 5000);
    if (!db) {
        fprintf(stderr, "Failed to initialize database\n");
        return 1;
    }
    printf("Database initialized\n");
    
    /* Insert some data */
    streamdb_insert(db, (unsigned char*)"cat", 3, "Hello, World!", 14);
    streamdb_insert(db, (unsigned char*)"car", 3, "Fast vehicle", 13);
    streamdb_insert(db, (unsigned char*)"cart", 4, "Shopping cart", 14);
    streamdb_insert(db, (unsigned char*)"bar", 3, "Drinks here", 12);
    printf("Inserted 4 entries\n");
    
    /* Retrieve a value */
    size_t size;
    char* value = (char*)streamdb_get(db, (unsigned char*)"cat", 3, &size);
    if (value) {
        printf("Get 'cat': %s\n", value);
        free(value);  /* Important: free the copy */
    }
    
    /* Check existence */
    printf("Exists 'car': %s\n", 
           streamdb_exists(db, (unsigned char*)"car", 3) ? "yes" : "no");
    printf("Exists 'dog': %s\n", 
           streamdb_exists(db, (unsigned char*)"dog", 3) ? "yes" : "no");
    
    printf("\n");
    
    /* ================================================================
     * Suffix Search
     * ================================================================ */
    printf("2. Suffix Search\n");
    printf("----------------\n");
    
    /* Find all keys ending with "ar" */
    printf("Keys ending with 'ar':\n");
    StreamDBResult* results = streamdb_suffix_search(db, (unsigned char*)"ar", 2);
    for (StreamDBResult* r = results; r; r = r->next) {
        printf("  %.*s -> %.*s\n", 
               (int)r->key_len, (char*)r->key,
               (int)r->value_size - 1, (char*)r->value);
    }
    streamdb_free_results(results);
    
    /* Find all keys ending with "art" */
    printf("Keys ending with 'art':\n");
    results = streamdb_suffix_search(db, (unsigned char*)"art", 3);
    for (StreamDBResult* r = results; r; r = r->next) {
        printf("  %.*s -> %.*s\n", 
               (int)r->key_len, (char*)r->key,
               (int)r->value_size - 1, (char*)r->value);
    }
    streamdb_free_results(results);
    
    printf("\n");
    
    /* ================================================================
     * Iteration
     * ================================================================ */
    printf("3. Iteration (foreach)\n");
    printf("----------------------\n");
    printf("All entries:\n");
    streamdb_foreach(db, print_entry, NULL);
    
    printf("\n");
    
    /* ================================================================
     * Statistics
     * ================================================================ */
    printf("4. Statistics\n");
    printf("-------------\n");
    StreamDBStats stats;
    streamdb_get_stats(db, &stats);
    printf("Key count: %zu\n", stats.key_count);
    printf("Total size: %zu bytes\n", stats.total_size);
    printf("Node count: %zu\n", stats.node_count);
    printf("Dirty: %s\n", stats.is_dirty ? "yes" : "no");
    printf("File backend: %s\n", stats.is_file_backend ? "yes" : "no");
    
    printf("\n");
    
    /* ================================================================
     * Delete
     * ================================================================ */
    printf("5. Delete Operation\n");
    printf("-------------------\n");
    
    StreamDBStatus status = streamdb_delete(db, (unsigned char*)"car", 3);
    printf("Delete 'car': %s\n", streamdb_strerror(status));
    
    status = streamdb_delete(db, (unsigned char*)"nonexistent", 11);
    printf("Delete 'nonexistent': %s\n", streamdb_strerror(status));
    
    printf("\n");
    
    /* ================================================================
     * Wrapper Functions
     * ================================================================ */
    printf("6. Wrapper Functions\n");
    printf("--------------------\n");
    
    /* Write document with GUID */
    char guid[37];
    status = streamdb_write_document(db, "/docs/readme.txt",
                                      "This is a document.", 20, guid);
    printf("Write document: %s (GUID: %s)\n", streamdb_strerror(status), guid);
    
    /* Read document */
    void* doc = streamdb_read_document(db, "/docs/readme.txt", &size);
    if (doc) {
        printf("Read document: %s\n", (char*)doc);
        free(doc);
    }
    
    /* Search wrapper */
    streamdb_write_document(db, "/docs/notes.txt", "Notes here", 11, guid);
    streamdb_write_document(db, "/images/photo.jpg", "Binary data", 12, guid);
    
    StreamDBSearchResults* search = streamdb_search(db, ".txt");
    if (search) {
        printf("Files ending with '.txt':\n");
        for (size_t i = 0; i < search->count; i++) {
            printf("  %s\n", search->paths[i]);
        }
        streamdb_free_search_results(search);
    }
    
    /* Batch write */
    StreamDBBatchEntry entries[] = {
        {"/batch/1", "First", 6},
        {"/batch/2", "Second", 7},
        {"/batch/3", "Third", 6},
    };
    status = streamdb_batch_write(db, entries, 3);
    printf("Batch write 3 entries: %s\n", streamdb_strerror(status));
    
    printf("\n");
    
    /* ================================================================
     * Persistence
     * ================================================================ */
    printf("7. Persistence\n");
    printf("--------------\n");
    
    /* Manual flush */
    status = streamdb_flush(db);
    printf("Flush: %s\n", streamdb_strerror(status));
    
    /* Cleanup */
    streamdb_free(db);
    printf("Database closed\n");
    
    /* Reopen and verify */
    db = streamdb_init("example.dat", 0);
    if (db) {
        printf("Reopened database\n");
        
        value = (char*)streamdb_get(db, (unsigned char*)"cat", 3, &size);
        if (value) {
            printf("Verified 'cat': %s\n", value);
            free(value);
        }
        
        streamdb_free(db);
    }
    
    /* Clean up file */
    remove("example.dat");
    
    printf("\n================================================\n");
    printf("Example completed successfully!\n");
    
    return 0;
}
