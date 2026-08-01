/*
 * c_writer - writes a known v3 database with the C implementation.
 *
 * Used by `make crosscheck`: the Rust test suite then opens the file and
 * verifies every key byte-for-byte (Rust/tests/cross_compat.rs).
 *
 * Usage: c_writer [output-path]   (default /tmp/streamdb-c-fixture.db)
 */

#include "streamdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv) {
    const char* path = argc > 1 ? argv[1] : "/tmp/streamdb-c-fixture.db";
    remove(path);

    StreamDB* db = streamdb_init(path, 0);
    if (!db) {
        fprintf(stderr, "c_writer: failed to open %s\n", path);
        return 1;
    }

    if (streamdb_insert(db, (const unsigned char*)"cross:a", 7,
                        "alpha-from-c", 12) != STREAMDB_OK ||
        streamdb_insert(db, (const unsigned char*)"cross:b", 7,
                        "beta-from-c", 11) != STREAMDB_OK) {
        fprintf(stderr, "c_writer: string inserts failed\n");
        return 1;
    }

    /* Binary payloads with deterministic content */
    {
        unsigned char png1[256];
        unsigned char png2[512];
        for (size_t i = 0; i < sizeof(png1); i++) png1[i] = (unsigned char)(i % 256);
        for (size_t i = 0; i < sizeof(png2); i++) png2[i] = (unsigned char)((i * 7) % 256);
        if (streamdb_insert(db, (const unsigned char*)"pic:one.png", 11,
                            png1, sizeof(png1)) != STREAMDB_OK ||
            streamdb_insert(db, (const unsigned char*)"pic:two.png", 11,
                            png2, sizeof(png2)) != STREAMDB_OK) {
            fprintf(stderr, "c_writer: binary inserts failed\n");
            return 1;
        }
    }

    if (streamdb_flush(db) != STREAMDB_OK) {
        fprintf(stderr, "c_writer: flush failed\n");
        return 1;
    }

    streamdb_free(db);
    printf("c_writer: wrote %s\n", path);
    return 0;
}
