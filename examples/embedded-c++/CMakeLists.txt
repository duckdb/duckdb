cmake_minimum_required(VERSION 2.8.7)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)

include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(example main.cpp)
target_link_libraries(example duckdb)
