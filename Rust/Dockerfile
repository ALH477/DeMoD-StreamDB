# syntax=docker/dockerfile:1
#
# StreamDB (Rust) — crate v2.0.0, on-disk format v3
# Reverse-trie embedded KV store with crash-safe append-only persistence.
# Ships: libstreamdb.so/.a (C FFI), streamdb.h, streamdb-cli.
# The build stage runs the test suite; the build fails if any test fails.
#
# Build:  docker build -t alh477/streamdb:2.0.0 Rust/
# Run:    docker run --rm alh477/streamdb:2.0.0 --help
#         docker run --rm -v /some/dir:/data alh477/streamdb:2.0.0 -d /data/db.dat stats

FROM rust:slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --features "ffi cli" \
    && cargo test --release --features "ffi cli" --quiet

FROM debian:bookworm-slim
COPY --from=build /src/target/release/libstreamdb.so /usr/local/lib/
COPY --from=build /src/target/release/libstreamdb.a /usr/local/lib/
COPY --from=build /src/target/release/streamdb-cli /usr/local/bin/streamdb-cli
COPY include/streamdb.h /usr/local/include/
RUN ldconfig
LABEL org.opencontainers.image.title="streamdb" \
      org.opencontainers.image.description="StreamDB (Rust) — reverse-trie embedded KV store. Crash-safe append-only v3 format, CRC32, compaction, C FFI + CLI." \
      org.opencontainers.image.source="https://github.com/ALH477/DeMoD-StreamDB" \
      org.opencontainers.image.licenses="LGPL-3.0-only"
ENTRYPOINT ["streamdb-cli"]
CMD ["--help"]
