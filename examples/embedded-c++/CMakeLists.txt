cmake_minimum_required(VERSION 2.8.12...3.29)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)

# Set include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src/include)

# Create an executable
add_executable(example main.cpp)

# Add the DuckDB library path and link (using the shared library)
target_link_libraries(example ${CMAKE_CURRENT_SOURCE_DIR}/../../build/release/src/libduckdb.dylib)
