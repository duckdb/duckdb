cmake_minimum_required(VERSION 2.8.12...3.29)
project(example-window)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

include_directories(../../src/include)
link_directories(../../build/debug/src)

add_executable(example main.cpp)
target_link_libraries(example duckdb)
