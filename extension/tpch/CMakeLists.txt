cmake_minimum_required(VERSION 2.8.12)

project(TPCHExtension)

include_directories(dbgen/include)
include_directories(include)
add_subdirectory(dbgen)

add_library(tpch_extension STATIC tpch-extension.cpp ${DBGEN_OBJECT_FILES})

set(INGORE_WARNINGS OFF)
build_loadable_extension(tpch tpch-extension.cpp ${DBGEN_OBJECT_FILES}
                         ${IGNORE_WARNINGS})

install(
  TARGETS tpch_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
