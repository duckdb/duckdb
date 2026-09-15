# Root CMakeLists of the published duckdb-cpp package. Assembled into the
# artifact by scripts/package_cpp_api.py; not used by the in-tree build.
cmake_minimum_required(VERSION 3.14...3.29)

project(duckdb_cpp LANGUAGES CXX)

include("${CMAKE_CURRENT_LIST_DIR}/cmake/duckdb_cpp_version.cmake")
set(DUCKDB_CPP_DIR "${CMAKE_CURRENT_LIST_DIR}")
include("${CMAKE_CURRENT_LIST_DIR}/cmake/DuckDBCppApi.cmake")
