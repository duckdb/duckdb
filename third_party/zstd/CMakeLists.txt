cmake_minimum_required(VERSION 3.5...3.29)

include_directories(include)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(
  duckdb_zstd STATIC
  decompress/zstd_ddict.cpp
  decompress/huf_decompress.cpp
  decompress/zstd_decompress.cpp
  decompress/zstd_decompress_block.cpp
  common/entropy_common.cpp
  common/fse_decompress.cpp
  common/zstd_common.cpp
  common/error_private.cpp
  common/xxhash.cpp
  compress/fse_compress.cpp
  compress/hist.cpp
  compress/huf_compress.cpp
  compress/zstd_compress.cpp
  compress/zstd_compress_literals.cpp
  compress/zstd_compress_sequences.cpp
  compress/zstd_compress_superblock.cpp
  compress/zstd_double_fast.cpp
  compress/zstd_fast.cpp
  compress/zstd_lazy.cpp
  compress/zstd_ldm.cpp)

target_include_directories(
        duckdb_zstd
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

install(TARGETS duckdb_zstd
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_zstd)
