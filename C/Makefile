# StreamDB Makefile
#
# Copyright (C) 2025 DeMoD LLC
# Licensed under LGPL-2.1

# Compiler settings
CC ?= gcc
AR ?= ar
CFLAGS ?= -Wall -Wextra -Werror -pedantic -std=c11 -O2
CFLAGS += -fPIC
DEBUG_CFLAGS = -g -O0 -DDEBUG -fsanitize=address -fsanitize=undefined

# Directories
SRCDIR = src
INCDIR = include
TESTDIR = tests
BUILDDIR = build
LIBDIR = lib

# Files
LIB_SRCS = $(SRCDIR)/streamdb.c $(SRCDIR)/streamdb_wrapper.c
LIB_OBJS = $(BUILDDIR)/streamdb.o $(BUILDDIR)/streamdb_wrapper.o
TEST_SRCS = $(TESTDIR)/test_streamdb.c

# Output
STATIC_LIB = $(LIBDIR)/libstreamdb.a
SHARED_LIB = $(LIBDIR)/libstreamdb.so
TEST_BIN = $(BUILDDIR)/test_streamdb

# Platform detection
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    SHARED_LIB = $(LIBDIR)/libstreamdb.dylib
    SHARED_FLAGS = -dynamiclib
    LDFLAGS += -lpthread
else ifeq ($(UNAME_S),Linux)
    SHARED_FLAGS = -shared
    LDFLAGS += -lpthread
else
    # Windows (MinGW)
    SHARED_LIB = $(LIBDIR)/libstreamdb.dll
    SHARED_FLAGS = -shared
endif

# Include path
INCLUDES = -I$(INCDIR)

# Targets
.PHONY: all clean test debug static shared install help

all: static shared

# Create directories
$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(LIBDIR):
	mkdir -p $(LIBDIR)

# Object files
$(BUILDDIR)/streamdb.o: $(SRCDIR)/streamdb.c $(INCDIR)/streamdb.h | $(BUILDDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILDDIR)/streamdb_wrapper.o: $(SRCDIR)/streamdb_wrapper.c $(INCDIR)/streamdb_wrapper.h $(INCDIR)/streamdb.h | $(BUILDDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Static library
static: $(STATIC_LIB)

$(STATIC_LIB): $(LIB_OBJS) | $(LIBDIR)
	$(AR) rcs $@ $^
	@echo "Built static library: $@"

# Shared library
shared: $(SHARED_LIB)

$(SHARED_LIB): $(LIB_OBJS) | $(LIBDIR)
	$(CC) $(SHARED_FLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Built shared library: $@"

# Test binary
$(TEST_BIN): $(TEST_SRCS) $(STATIC_LIB) | $(BUILDDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< -L$(LIBDIR) -lstreamdb $(LDFLAGS)

# Run tests
test: $(TEST_BIN)
	@echo ""
	@echo "Running tests..."
	@echo ""
	@./$(TEST_BIN)

# Debug build
debug: CFLAGS = $(DEBUG_CFLAGS)
debug: clean all test

# Install (Unix-like systems)
PREFIX ?= /usr/local

install: all
	install -d $(DESTDIR)$(PREFIX)/lib
	install -d $(DESTDIR)$(PREFIX)/include
	install -m 644 $(STATIC_LIB) $(DESTDIR)$(PREFIX)/lib/
	install -m 755 $(SHARED_LIB) $(DESTDIR)$(PREFIX)/lib/
	install -m 644 $(INCDIR)/streamdb.h $(DESTDIR)$(PREFIX)/include/
	install -m 644 $(INCDIR)/streamdb_wrapper.h $(DESTDIR)$(PREFIX)/include/
	@echo "Installed to $(PREFIX)"

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/lib/libstreamdb.*
	rm -f $(DESTDIR)$(PREFIX)/include/streamdb.h
	rm -f $(DESTDIR)$(PREFIX)/include/streamdb_wrapper.h

# Clean
clean:
	rm -rf $(BUILDDIR) $(LIBDIR)
	rm -f /tmp/streamdb_test_*.dat

# Help
help:
	@echo "StreamDB Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all      - Build static and shared libraries (default)"
	@echo "  static   - Build static library only"
	@echo "  shared   - Build shared library only"
	@echo "  test     - Build and run tests"
	@echo "  debug    - Build with debug flags and run tests"
	@echo "  install  - Install to PREFIX (default: /usr/local)"
	@echo "  clean    - Remove build artifacts"
	@echo "  help     - Show this message"
	@echo ""
	@echo "Variables:"
	@echo "  CC       - Compiler (default: gcc)"
	@echo "  CFLAGS   - Compiler flags"
	@echo "  PREFIX   - Install prefix (default: /usr/local)"
