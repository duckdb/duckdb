cmake_minimum_required(VERSION 2.8.7)

project(ParquetExtension)

include_directories(include ../../third_party/parquet ../../third_party/snappy
                    ../../third_party/thrift ../../third_party)

set(PARQUET_SOURCES ../../third_party/parquet/parquet_constants.cpp
  ../../third_party/parquet/parquet_types.cpp
  ../../third_party/thrift/protocol/TProtocol.cpp
  ../../third_party/thrift/transport/TTransportException.cpp
  ../../third_party/thrift/transport/TBufferTransports.cpp
  ../../third_party/snappy/snappy.cc
  ../../third_party/snappy/snappy-sinksource.cc
  parquet-extension.cpp)

add_library(parquet_extension_static STATIC ${PARQUET_SOURCES})
add_library(parquet_extension ${PARQUET_SOURCES})


add_subdirectory(test)
