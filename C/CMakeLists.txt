# StreamDB CMake Build
#
# Copyright (C) 2025 DeMoD LLC
# Licensed under LGPL-2.1

cmake_minimum_required(VERSION 3.10)

project(StreamDB
    VERSION 2.0.0
    DESCRIPTION "A lightweight, thread-safe embedded database using reverse trie"
    LANGUAGES C
)

# C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Options
option(STREAMDB_BUILD_SHARED "Build shared library" ON)
option(STREAMDB_BUILD_STATIC "Build static library" ON)
option(STREAMDB_BUILD_TESTS "Build test suite" ON)
option(STREAMDB_INSTALL "Generate install target" ON)

# Compiler warnings
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Werror -pedantic)
elseif(MSVC)
    add_compile_options(/W4 /WX)
endif()

# Source files
set(STREAMDB_SOURCES
    src/streamdb.c
    src/streamdb_wrapper.c
)

set(STREAMDB_HEADERS
    include/streamdb.h
    include/streamdb_wrapper.h
)

# Include directory
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Platform-specific settings
if(WIN32)
    # Windows: link against rpcrt4 for UUID generation
    set(PLATFORM_LIBS rpcrt4)
elseif(UNIX)
    # Unix: link against pthread
    find_package(Threads REQUIRED)
    set(PLATFORM_LIBS Threads::Threads)
endif()

# Static library
if(STREAMDB_BUILD_STATIC)
    add_library(streamdb_static STATIC ${STREAMDB_SOURCES})
    target_include_directories(streamdb_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(streamdb_static PUBLIC ${PLATFORM_LIBS})
    set_target_properties(streamdb_static PROPERTIES
        OUTPUT_NAME streamdb
        PUBLIC_HEADER "${STREAMDB_HEADERS}"
    )
endif()

# Shared library
if(STREAMDB_BUILD_SHARED)
    add_library(streamdb_shared SHARED ${STREAMDB_SOURCES})
    target_include_directories(streamdb_shared PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_link_libraries(streamdb_shared PUBLIC ${PLATFORM_LIBS})
    set_target_properties(streamdb_shared PROPERTIES
        OUTPUT_NAME streamdb
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
        PUBLIC_HEADER "${STREAMDB_HEADERS}"
    )
    
    # Windows DLL export
    if(WIN32)
        target_compile_definitions(streamdb_shared PRIVATE STREAMDB_EXPORTS)
    endif()
endif()

# Alias for easier linking
if(STREAMDB_BUILD_STATIC)
    add_library(StreamDB::streamdb ALIAS streamdb_static)
elseif(STREAMDB_BUILD_SHARED)
    add_library(StreamDB::streamdb ALIAS streamdb_shared)
endif()

# Test executable
if(STREAMDB_BUILD_TESTS)
    enable_testing()
    
    add_executable(test_streamdb tests/test_streamdb.c)
    
    if(STREAMDB_BUILD_STATIC)
        target_link_libraries(test_streamdb PRIVATE streamdb_static)
    else()
        target_link_libraries(test_streamdb PRIVATE streamdb_shared)
    endif()
    
    add_test(NAME StreamDBTests COMMAND test_streamdb)
    
    # Debug build with sanitizers (GCC/Clang)
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
            target_compile_options(test_streamdb PRIVATE 
                -fsanitize=address -fsanitize=undefined)
            target_link_options(test_streamdb PRIVATE 
                -fsanitize=address -fsanitize=undefined)
        endif()
    endif()
endif()

# Installation
if(STREAMDB_INSTALL)
    include(GNUInstallDirs)
    
    # Install libraries
    if(STREAMDB_BUILD_STATIC)
        install(TARGETS streamdb_static
            EXPORT StreamDBTargets
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )
    endif()
    
    if(STREAMDB_BUILD_SHARED)
        install(TARGETS streamdb_shared
            EXPORT StreamDBTargets
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
            PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )
    endif()
    
    # Install CMake config files
    install(EXPORT StreamDBTargets
        FILE StreamDBTargets.cmake
        NAMESPACE StreamDB::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/StreamDB
    )
    
    # Generate and install config file
    include(CMakePackageConfigHelpers)
    
    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/StreamDBConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/StreamDBConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/StreamDB
    )
    
    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/StreamDBConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/StreamDBConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/StreamDBConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/StreamDB
    )
    
    # pkg-config file
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/streamdb.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/streamdb.pc
        @ONLY
    )
    
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/streamdb.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    )
endif()

# Summary
message(STATUS "")
message(STATUS "StreamDB ${PROJECT_VERSION} Configuration:")
message(STATUS "  Build static library: ${STREAMDB_BUILD_STATIC}")
message(STATUS "  Build shared library: ${STREAMDB_BUILD_SHARED}")
message(STATUS "  Build tests: ${STREAMDB_BUILD_TESTS}")
message(STATUS "  Install: ${STREAMDB_INSTALL}")
message(STATUS "")
