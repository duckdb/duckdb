cmake_minimum_required(VERSION 2.8.12...3.29)

project(TpchExtension)

include_directories(dbgen/include)
include_directories(include)
add_subdirectory(dbgen)

build_static_extension(tpch tpch_extension.cpp ${DBGEN_OBJECT_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(tpch ${PARAMETERS} tpch_extension.cpp
                         ${DBGEN_OBJECT_FILES})

install(
  TARGETS tpch_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
