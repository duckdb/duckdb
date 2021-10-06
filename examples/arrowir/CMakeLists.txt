cmake_minimum_required(VERSION 2.8.12)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)

include_directories(schema/format)
include_directories(schema/ir)
include_directories(flatbuffers)
include_directories(../../extension/tpch/include)

include_directories(../../src/include)
include_directories(../../test/include)

link_directories(../../build/debug/src)
include_directories(../../third_party/catch)

link_directories(../../build/debug/extension/tpch)

add_executable(example test_arrowir.cpp ../../test/helpers/test_helpers.cpp)
target_link_libraries(example duckdb tpch_extension)
