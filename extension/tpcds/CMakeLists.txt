cmake_minimum_required(VERSION 3.5...3.29)

project(TpcdsExtension)

# The tpcds extension is written against DuckDB's stable C++ API only
# (tools/cpp/duckdb_cpp.hpp): it includes nothing of the engine. It is built
# twice from the same sources: tpcds_extension, a static archive linked into the
# DuckDB binaries, and the loadable tpcds.duckdb_extension, which routes every
# engine call through the extension vtable.

# Before any subdirectory or target, which would otherwise inherit the
# directory-wide version definition; the version comes from the describe
# function instead
set_extension_version_sources(tpcds)

set(TPCDS_INCLUDE_DIRS
    ${CMAKE_CURRENT_LIST_DIR}/dsdgen/include
    ${CMAKE_CURRENT_LIST_DIR}/dsdgen/include/dsdgen-c
    ${DUCKDB_MODULE_BASE_DIR}/tools/cpp)
include_directories(${TPCDS_INCLUDE_DIRS})
add_subdirectory(dsdgen)

# Static flavor: part of the DuckDB binaries
build_static_extension_capi_unstable(tpcds "" tpcds_extension.cpp
                                     ${DSDGEN_OBJECT_FILES})
target_link_libraries(tpcds_extension duckdb_cpp_api)

# Loadable flavor
if(NOT EMSCRIPTEN AND NOT WASM_LOADABLE_EXTENSIONS)
  build_loadable_extension_capi_unstable(tpcds "" tpcds_extension.cpp
                                         ${DSDGEN_OBJECT_FILES})
  target_compile_definitions(tpcds_loadable_extension
                             PRIVATE DUCKDB_CPP_API_LOADABLE)
  target_link_libraries(tpcds_loadable_extension duckdb_cpp_api_loadable)
endif()

install(
  TARGETS tpcds_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
