cmake_minimum_required(VERSION 2.8.7)

project(ParquetExtension)

include_directories(include ../../third_party/parquet ../../third_party/snappy
                    ../../third_party/miniz ../../third_party/thrift ../../third_party/zstd)

add_library(
  parquet_extension STATIC
  ../../third_party/parquet/parquet_constants.cpp
  ../../third_party/parquet/parquet_types.cpp
  ../../third_party/thrift/thrift/protocol/TProtocol.cpp
  ../../third_party/thrift/thrift/transport/TTransportException.cpp
  ../../third_party/thrift/thrift/transport/TBufferTransports.cpp
  ../../third_party/snappy/snappy.cc
  ../../third_party/snappy/snappy-sinksource.cc
  ../../third_party/zstd/decompress/zstd_ddict.cpp
  ../../third_party/zstd/decompress/huf_decompress.cpp
  ../../third_party/zstd/decompress/zstd_decompress.cpp
  ../../third_party/zstd/decompress/zstd_decompress_block.cpp
  ../../third_party/zstd/common/entropy_common.cpp
  ../../third_party/zstd/common/fse_decompress.cpp
  ../../third_party/zstd/common/zstd_common.cpp
  ../../third_party/zstd/common/error_private.cpp
  ../../third_party/zstd/common/xxhash.cpp
  parquet-extension.cpp
  parquet_reader.cpp
  parquet_timestamp.cpp
  parquet_writer.cpp)
