################################################################################
# DuckDB extension base config
################################################################################
#
# This is the base DuckDB extension configuration file. The extensions loaded here are included in every DuckDB build.
# Note that this file is checked into version control; if you want to specify which extensions to load for local
# development, create `extension/extension_config_local.cmake` and specify extensions there.
# The local file is also loaded by the DuckDB CMake build but ignored by version control.

# these extensions are loaded by default on every build as they are an essential part of DuckDB
duckdb_extension_load(core_functions)
duckdb_extension_load(parquet)


# The Linux allocator has issues so we use jemalloc, but only on x86 because page sizes are fixed at 4KB.
# Configuring jemalloc properly for 32bit is a hassle, and not worth it so we only enable on 64bit
if(CMAKE_SIZEOF_VOID_P EQUAL 8 AND NOT FORCE_32_BIT AND OS_NAME STREQUAL "linux" AND NOT WASM_LOADABLE_EXTENSIONS AND NOT CLANG_TIDY AND NOT ANDROID AND NOT ZOS AND NOT BSD AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(jemalloc)
endif()
