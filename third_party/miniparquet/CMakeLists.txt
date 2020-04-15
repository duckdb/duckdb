cmake_minimum_required(VERSION 2.8.7)

project(miniparquet CXX C)

include_directories(.
                    parquet
                    snappy
                    thrift)


add_library_unity(miniparquet_sources OBJECT
            parquet/parquet_constants.cpp
            parquet/parquet_types.cpp
            thrift/protocol/TProtocol.cpp
            thrift/transport/TTransportException.cpp
            thrift/transport/TBufferTransports.cpp
            snappy/snappy.cc
            snappy/snappy-sinksource.cc
            miniparquet.cpp
            duckdb_miniparquet.cpp)

add_library_unity(miniparquet STATIC $<TARGET_OBJECTS:miniparquet_sources>)