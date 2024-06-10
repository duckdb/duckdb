cmake_minimum_required(VERSION 3.5...3.29)

project(ParquetExtension)

include_directories(
  include
  ../../third_party/lz4
  ../../third_party/parquet
  ../../third_party/thrift
  ../../third_party/snappy
  ../../third_party/zstd/include
  ../../third_party/mbedtls
  ../../third_party/mbedtls/include)

set(PARQUET_EXTENSION_FILES
    column_reader.cpp
    column_writer.cpp
    parquet_crypto.cpp
    parquet_extension.cpp
    parquet_metadata.cpp
    parquet_reader.cpp
    parquet_statistics.cpp
    parquet_timestamp.cpp
    parquet_writer.cpp
    serialize_parquet.cpp
    zstd_file_system.cpp)

if(NOT CLANG_TIDY)
  # copy over checks from snappy
  include(CheckCXXSourceCompiles)
  check_cxx_source_compiles(
    "
  int main() {
    return __builtin_expect(0, 1);
  }"
    HAVE_BUILTIN_EXPECT)

  check_cxx_source_compiles(
    "
  int main() {
    return __builtin_ctzll(0);
  }"
    HAVE_BUILTIN_CTZ)

  check_cxx_source_compiles(
    "
  int main() {
    __builtin_prefetch(0, 0, 3);
    return 0;
  }"
    HAVE_BUILTIN_PREFETCH)

  check_cxx_source_compiles(
    "
  __attribute__((always_inline)) int zero() { return 0; }

  int main() {
    return zero();
  }"
    HAVE_ATTRIBUTE_ALWAYS_INLINE)

  if(HAVE_BUILTIN_EXPECT)
    add_definitions(-DHAVE_BUILTIN_EXPECT)
  endif()
  if(HAVE_BUILTIN_CTZ)
    add_definitions(-DHAVE_BUILTIN_CTZ)
  endif()
  if(HAVE_BUILTIN_PREFETCH)
    add_definitions(-DHAVE_BUILTIN_PREFETCH)
  endif()
  if(HAVE_ATTRIBUTE_ALWAYS_INLINE)
    add_definitions(-DHAVE_ATTRIBUTE_ALWAYS_INLINE)
  endif()

  include(CheckSymbolExists)
  check_symbol_exists("mmap" "sys/mman.h" HAVE_FUNC_MMAP)
  check_symbol_exists("sysconf" "unistd.h" HAVE_FUNC_SYSCONF)

  if(HAVE_FUNC_MMAP)
    add_definitions(-DHAVE_FUNC_MMAP)
  endif()
  if(HAVE_FUNC_SYSCONF)
    add_definitions(-DHAVE_FUNC_SYSCONF)
  endif()

  # parquet/thrift/snappy
  set(PARQUET_EXTENSION_FILES
      ${PARQUET_EXTENSION_FILES}
      ../../third_party/parquet/parquet_constants.cpp
      ../../third_party/parquet/parquet_types.cpp
      ../../third_party/thrift/thrift/protocol/TProtocol.cpp
      ../../third_party/thrift/thrift/transport/TTransportException.cpp
      ../../third_party/thrift/thrift/transport/TBufferTransports.cpp
      ../../third_party/snappy/snappy.cc
      ../../third_party/snappy/snappy-sinksource.cc)
  # zstd
  set(PARQUET_EXTENSION_FILES
      ${PARQUET_EXTENSION_FILES}
      ../../third_party/lz4/lz4.cpp
      ../../third_party/zstd/decompress/zstd_ddict.cpp
      ../../third_party/zstd/decompress/huf_decompress.cpp
      ../../third_party/zstd/decompress/zstd_decompress.cpp
      ../../third_party/zstd/decompress/zstd_decompress_block.cpp
      ../../third_party/zstd/common/entropy_common.cpp
      ../../third_party/zstd/common/fse_decompress.cpp
      ../../third_party/zstd/common/zstd_common.cpp
      ../../third_party/zstd/common/error_private.cpp
      ../../third_party/zstd/common/xxhash.cpp
      ../../third_party/zstd/compress/fse_compress.cpp
      ../../third_party/zstd/compress/hist.cpp
      ../../third_party/zstd/compress/huf_compress.cpp
      ../../third_party/zstd/compress/zstd_compress.cpp
      ../../third_party/zstd/compress/zstd_compress_literals.cpp
      ../../third_party/zstd/compress/zstd_compress_sequences.cpp
      ../../third_party/zstd/compress/zstd_compress_superblock.cpp
      ../../third_party/zstd/compress/zstd_double_fast.cpp
      ../../third_party/zstd/compress/zstd_fast.cpp
      ../../third_party/zstd/compress/zstd_lazy.cpp
      ../../third_party/zstd/compress/zstd_ldm.cpp
      ../../third_party/zstd/compress/zstd_opt.cpp)
endif()

build_static_extension(parquet ${PARQUET_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(parquet ${PARAMETERS} ${PARQUET_EXTENSION_FILES})
target_link_libraries(parquet_loadable_extension duckdb_mbedtls)

install(
  TARGETS parquet_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
