cmake_minimum_required(VERSION 3.12.0)
find_package(ODBC)

if(NOT ODBC_FOUND)
  message(FATAL_ERROR No ODBC found)
endif()

add_definitions(-DNO_FRAMEWORKS)
message(${ODBC_INCLUDE_DIRS})
include_directories(${ODBC_INCLUDE_DIRS})

add_library(duckdb_odbc SHARED duckdb_odbc.cpp)
target_link_libraries(duckdb_odbc duckdb_static)

add_executable(odbc_example example.cpp)
target_link_libraries(odbc_example ${ODBC_LIBRARIES})
