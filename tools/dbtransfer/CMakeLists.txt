cmake_minimum_required(VERSION 2.8.7)

project(dbtransfer CXX C)

include_directories(include)
include_directories(../../third_party/sqlite/include)

add_library(sqlite_transfer STATIC sqlite_transfer.cpp)
target_link_libraries(sqlite_transfer duckdb sqlite3)
