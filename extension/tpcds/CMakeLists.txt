cmake_minimum_required(VERSION 2.8.12)

project(TPCDSExtension)

include_directories(include)
include_directories(dsdgen/include)
add_subdirectory(dsdgen)

add_library(tpcds_extension STATIC tpcds-extension.cpp ${DSDGEN_OBJECT_FILES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(tpcds tpcds-extension.cpp ${DSDGEN_OBJECT_FILES}
                         ${IGNORE_WARNINGS})

install(
  TARGETS tpcds_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
