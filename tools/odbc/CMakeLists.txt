cmake_minimum_required(VERSION 3.12.0)
find_package(ODBC)

if(NOT ODBC_FOUND)
  message(FATAL_ERROR No ODBC found)
endif()

add_definitions(-DNO_FRAMEWORKS)
include_directories(${ODBC_INCLUDE_DIRS} include)

add_library(
  duckdb_odbc SHARED
  odbc_fetch.cpp # should be first because of foward declaration, i.e., circular
                 # dependencies with duckdb_odbc.cpp
  duckdb_odbc.cpp
  driver.cpp
  connection.cpp
  statement.cpp
  prepared.cpp
  result.cpp
  api_info.cpp
  statement_functions.cpp
  odbc_interval.cpp)

set_target_properties(duckdb_odbc PROPERTIES DEFINE_SYMBOL "DUCKDB_ODBC_API")

target_link_libraries(duckdb_odbc duckdb_static)
