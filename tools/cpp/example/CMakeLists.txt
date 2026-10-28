# Minimal external consumer of the duckdb-cpp package. This is the README
# snippet, unchanged: for a local dry-run against an assembled package, pass
# -DFETCHCONTENT_SOURCE_DIR_DUCKDB_CPP=<package dir> (CMake's standard local
# override) instead of letting FetchContent clone the repository.
cmake_minimum_required(VERSION 3.14...3.29)

project(duckdb_cpp_example LANGUAGES CXX)

include(FetchContent)
FetchContent_Declare(
  duckdb_cpp
  GIT_REPOSITORY https://github.com/duckdb/duckdb-cpp
  GIT_TAG v0.1.0)
FetchContent_MakeAvailable(duckdb_cpp)

add_executable(example main.cpp)
target_link_libraries(example PRIVATE duckdb::cpp_api)
