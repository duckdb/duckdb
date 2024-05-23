# Set the minimum required CMake version
cmake_minimum_required(VERSION 3.12)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_COMPILER g++)
project(test_query)

# DuckDB
set(DUCKDB_DIR "/home/cjoy/src/duckdb/build/debug/src")
set(DUCKDB_INCLUDE_DIR "/home/cjoy/src/duckdb/src/include")
# Check if the libduckdb library is found
find_library(DUCKDB_LIB duckdb PATHS ${DUCKDB_DIR})
if(NOT DUCKDB_LIB)
    message(FATAL_ERROR "libduckdb not found. Please check the DUCKDB_DIR environment variable.")
else()
    message(STATUS "libduckdb found at ${DUCKDB_LIB}")
endif()

add_executable(${PROJECT_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

# Specify include and link directories
target_include_directories(${PROJECT_NAME} PRIVATE ${DUCKDB_INCLUDE_DIR} ${DUCKDB_DIR})

# Link the DuckDB library
target_link_libraries(${PROJECT_NAME} PRIVATE ${DUCKDB_DIR}/libduckdb.so)