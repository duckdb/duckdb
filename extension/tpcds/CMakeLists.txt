cmake_minimum_required(VERSION 2.8.12...3.29)

project(TpcdsExtension)

# Before any subdirectory or target, which would otherwise inherit the
# directory-wide definition
set_extension_version_sources(tpcds tpcds_extension.cpp)

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
