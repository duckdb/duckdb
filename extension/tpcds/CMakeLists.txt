cmake_minimum_required(VERSION 2.8.12)

project(TpcdsExtension)

include_directories(include)
include_directories(dsdgen/include)
add_subdirectory(dsdgen)

build_static_extension(tpcds tpcds_extension.cpp ${DSDGEN_OBJECT_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(tpcds ${PARAMETERS} tpcds_extension.cpp
                         ${DSDGEN_OBJECT_FILES})

install(
  TARGETS tpcds_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
