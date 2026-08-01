# syntax=docker/dockerfile:1
#
# StreamDB (C) — v3.0.0
# Reverse-trie embedded KV store, pure C11, zero dependencies.
# Ships: libstreamdb.a/.so, public headers, streamdb-selftest.
# The build stage runs the full 30-test suite; the build fails if any test fails.
#
# Build:  docker build -t alh477/streamdb-c:3.0.0 C/
# Run:    docker run --rm alh477/streamdb-c:3.0.0   (runs the self-test)

FROM debian:bookworm-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc make libc6-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src ./src
COPY include ./include
COPY tests ./tests
COPY Makefile ./
RUN make static shared && make test

FROM debian:bookworm-slim
COPY --from=build /src/lib/libstreamdb.a /usr/local/lib/
COPY --from=build /src/lib/libstreamdb.so /usr/local/lib/
COPY --from=build /src/include/streamdb.h /usr/local/include/
COPY --from=build /src/include/streamdb_wrapper.h /usr/local/include/
COPY --from=build /src/build/test_streamdb /usr/local/bin/streamdb-selftest
RUN ldconfig
LABEL org.opencontainers.image.title="streamdb-c" \
      org.opencontainers.image.description="StreamDB (C) v3.0.0 — reverse-trie embedded KV store. Crash-safe v3 format, byte-compatible with the Rust implementation. Pure C11, zero deps." \
      org.opencontainers.image.source="https://github.com/ALH477/DeMoD-StreamDB" \
      org.opencontainers.image.licenses="LGPL-2.1-or-later"
ENTRYPOINT ["/usr/local/bin/streamdb-selftest"]
