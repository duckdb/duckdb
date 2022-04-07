cmake_minimum_required(VERSION 2.8.12)

project(SQLSmithExtension)

include_directories(include)
include_directories(third_party/sqlsmith/include)
add_subdirectory(third_party)

add_library(sqlsmith_extension STATIC sqlsmith-extension.cpp
                                      ${SQLSMITH_OBJECT_FILES})

build_loadable_extension(sqlsmith sqlsmith-extension.cpp
                         ${SQLSMITH_OBJECT_FILES})

install(
  TARGETS sqlsmith_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
