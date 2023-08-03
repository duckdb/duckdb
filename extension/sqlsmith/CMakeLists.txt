cmake_minimum_required(VERSION 2.8.12)

project(SQLSmithExtension)

include_directories(include)
include_directories(third_party/sqlsmith/include)
add_subdirectory(third_party)

set(SQLSMITH_SOURCES
    sqlsmith_extension.cpp statement_generator.cpp statement_simplifier.cpp
    fuzzyduck.cpp ${SQLSMITH_OBJECT_FILES})

build_static_extension(sqlsmith ${SQLSMITH_SOURCES})
set(PARAMETERS "-warnings")
build_loadable_extension(sqlsmith ${PARAMETERS} ${SQLSMITH_SOURCES})

install(
  TARGETS sqlsmith_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
