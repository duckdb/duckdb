cmake_minimum_required(VERSION 2.8.12)

project(JSONExtension)

include_directories(yyjson/include)
include_directories(include)
add_subdirectory(yyjson)

add_library(json_extension STATIC json-extension.cpp json_extract.cpp
                                  ${YYJSON_OBJECT_FILES})

build_loadable_extension(json json-extension.cpp ${YYJSON_OBJECT_FILES})

install(
  TARGETS json_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
