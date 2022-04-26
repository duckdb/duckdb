cmake_minimum_required(VERSION 2.8.12)

project(SQLSmithExtension)

include_directories(include)
include_directories(third_party/sqlsmith/include)
add_subdirectory(third_party)

set(SQLSMITH_SOURCES sqlsmith-extension.cpp statement_simplifier.cpp
                     ${SQLSMITH_OBJECT_FILES})

add_library(sqlsmith_extension STATIC ${SQLSMITH_SOURCES})

build_loadable_extension(sqlsmith ${SQLSMITH_SOURCES})

install(
  TARGETS sqlsmith_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
