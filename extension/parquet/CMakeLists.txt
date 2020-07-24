cmake_minimum_required(VERSION 2.8.7)

project(ParquetExtension)

include_directories(include ../../third_party/parquet ../../third_party/snappy ../../third_party/miniz
                    ../../third_party/thrift ../../third_party)

add_library(
  parquet_extension STATIC
  ../../third_party/parquet/parquet_constants.cpp
  ../../third_party/parquet/parquet_types.cpp
  ../../third_party/thrift/protocol/TProtocol.cpp
  ../../third_party/thrift/transport/TTransportException.cpp
  ../../third_party/thrift/transport/TBufferTransports.cpp
  ../../third_party/snappy/snappy.cc
  ../../third_party/snappy/snappy-sinksource.cc
  parquet-extension.cpp)

add_subdirectory(test)
