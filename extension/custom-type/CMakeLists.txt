cmake_minimum_required(VERSION 2.8.12)

project(CustomTypeExtension)

include_directories(include)

add_library(custom_type_extension STATIC custom-type-extension.cpp)

build_loadable_extension(custom_type custom-type-extension.cpp)
install(
  TARGETS custom_type_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
