################################################################################
# DuckDB extension base config
################################################################################
#
# This is the base DuckDB extension configuration file. The extensions loaded here are included in every DuckDB build.
# Note that this file is checked into version control; if you want to specify which extensions to load for local
# development, create `extension/extension_config_local.cmake` and specify extensions there.
# The local file is also loaded by the DuckDB CMake build but ignored by version control.

# Parquet is loaded by default on every build as its a essential part of DuckDB
duckdb_extension_load(parquet)
duckdb_extension_load(kafkaredo)

# The Linux allocator has issues so we use jemalloc, but only on x86 because page sizes are fixed at 4KB.
# If page sizes vary for an architecture (e.g., arm64), we cannot create a portable binary due to jemalloc config
if(OS_NAME STREQUAL "linux" AND (OS_ARCH STREQUAL "amd64" OR OS_ARCH STREQUAL "i386") AND NOT WASM_LOADABLE_EXTENSIONS AND NOT CLANG_TIDY AND NOT ANDROID AND NOT ZOS)
    duckdb_extension_load(jemalloc)
endif()
