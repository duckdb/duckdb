cmake_minimum_required(VERSION 2.8.12...3.29)
project(example-c)

include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(example main.c)
target_link_libraries(example duckdb)
