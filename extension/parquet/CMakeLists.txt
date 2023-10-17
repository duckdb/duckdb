cmake_minimum_required(VERSION 2.8.12)

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
  # mbedtls
  set(PARQUET_EXTENSION_FILES
      ${PARQUET_EXTENSION_FILES}
      ../../third_party/mbedtls/library/aes.cpp
      ../../third_party/mbedtls/library/aria.cpp
      ../../third_party/mbedtls/library/asn1parse.cpp
      ../../third_party/mbedtls/library/base64.cpp
      ../../third_party/mbedtls/library/bignum.cpp
      ../../third_party/mbedtls/library/camellia.cpp
      ../../third_party/mbedtls/library/cipher.cpp
      ../../third_party/mbedtls/library/cipher_wrap.cpp
      ../../third_party/mbedtls/library/constant_time.cpp
      ../../third_party/mbedtls/library/entropy.cpp
      ../../third_party/mbedtls/library/entropy_poll.cpp
      ../../third_party/mbedtls/library/gcm.cpp
      ../../third_party/mbedtls/library/md.cpp
      ../../third_party/mbedtls/library/oid.cpp
      ../../third_party/mbedtls/library/pem.cpp
      ../../third_party/mbedtls/library/pk.cpp
      ../../third_party/mbedtls/library/pk_wrap.cpp
      ../../third_party/mbedtls/library/pkparse.cpp
      ../../third_party/mbedtls/library/platform_util.cpp
      ../../third_party/mbedtls/library/rsa.cpp
      ../../third_party/mbedtls/library/rsa_alt_helpers.cpp
      ../../third_party/mbedtls/library/sha1.cpp
      ../../third_party/mbedtls/library/sha256.cpp
      ../../third_party/mbedtls/library/sha512.cpp)
endif()

build_static_extension(parquet ${PARQUET_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(parquet ${PARAMETERS} ${PARQUET_EXTENSION_FILES})

install(
  TARGETS parquet_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
