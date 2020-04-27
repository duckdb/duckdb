cmake_minimum_required(VERSION 2.8.7)

project(miniparquet CXX C)

include_directories(.
                    parquet
                    snappy
                    thrift)


add_library(miniparquet STATIC
            parquet/parquet_constants.cpp
            parquet/parquet_types.cpp
            thrift/protocol/TProtocol.cpp
            thrift/transport/TTransportException.cpp
            thrift/transport/TBufferTransports.cpp
            snappy/snappy.cc
            snappy/snappy-sinksource.cc
            miniparquet.cpp
            duckdb_miniparquet.cpp)
