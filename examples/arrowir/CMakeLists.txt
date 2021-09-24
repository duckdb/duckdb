cmake_minimum_required(VERSION 2.8.12)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)

include_directories(schema/format)
include_directories(schema/ir)
include_directories(flatbuffers)
include_directories(../../extension/tpch/include)

include_directories(../../src/include)
link_directories(../../build/debug/src)
link_directories(../../build/debug/extension/tpch)

add_executable(example test_arrowir.cpp)
target_link_libraries(example duckdb)
target_link_libraries(example tpch_extension)
