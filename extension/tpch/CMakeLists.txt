cmake_minimum_required(VERSION 3.5...3.29)

project(TpchExtension)

# The tpch extension is written against DuckDB's stable C++ API only
# (tools/cpp/duckdb_cpp.hpp): it includes nothing of the engine. It is built
# twice from the same sources: tpch_extension, a static archive linked into the
# DuckDB binaries, and tpch_loadable_extension (tpch.duckdb_extension), which
# routes every engine call through the extension vtable.

# Drops the directory-wide version definition: the version is in the describe
# function and the loadable footer
set_extension_version_sources(tpch)

set(TPCH_INCLUDE_DIRS ${CMAKE_CURRENT_LIST_DIR}/dbgen/include
                      ${DUCKDB_MODULE_BASE_DIR}/tools/cpp)

# The generator, shared by both flavors: it only uses the header-only types of
# the C++ API
add_library(
  tpch_dbgen OBJECT
  dbgen/dbgen.cpp
  dbgen/dbgen_gunk.cpp
  dbgen/bm_utils.cpp
  dbgen/build.cpp
  dbgen/permute.cpp
  dbgen/rnd.cpp
  dbgen/rng64.cpp
  dbgen/speed_seed.cpp
  dbgen/text.cpp)
target_include_directories(tpch_dbgen PRIVATE ${TPCH_INCLUDE_DIRS})
set_target_properties(tpch_dbgen PROPERTIES POSITION_INDEPENDENT_CODE ON)
disable_target_warnings(tpch_dbgen)

set(TPCH_EXTENSION_FILES tpch_extension.cpp $<TARGET_OBJECTS:tpch_dbgen>)

build_static_extension_capi_unstable(tpch "" ${TPCH_EXTENSION_FILES})
target_include_directories(tpch_extension PRIVATE ${TPCH_INCLUDE_DIRS})
target_link_libraries(tpch_extension duckdb_cpp_api)

if(NOT EMSCRIPTEN AND NOT WASM_LOADABLE_EXTENSIONS)
  build_loadable_extension_capi_unstable(tpch "" ${TPCH_EXTENSION_FILES})
  target_compile_definitions(tpch_loadable_extension
                             PRIVATE DUCKDB_CPP_API_LOADABLE)
  target_include_directories(tpch_loadable_extension
                             PRIVATE ${TPCH_INCLUDE_DIRS})
  target_link_libraries(tpch_loadable_extension duckdb_cpp_api_loadable)
endif()

install(
  TARGETS tpch_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
