cmake_minimum_required(VERSION 3.5)

project(ParquetExtension)

include_directories(
  include
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

install(
  TARGETS parquet_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
