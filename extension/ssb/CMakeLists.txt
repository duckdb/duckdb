cmake_minimum_required(VERSION 2.8.12...3.29)

project(SSBExtension)

include_directories(ssbgen/include)
include_directories(include)
add_subdirectory(ssbgen)

build_static_extension(ssb ssb_extension.cpp ssb_dbgen.cpp ${SSBGEN_OBJECT_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(ssb ${PARAMETERS} ssb_extension.cpp
                         ${SSBGEN_OBJECT_FILES})

install(
  TARGETS ssb_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")