cmake_minimum_required(VERSION 2.8.12)

project(CustomTypeExtension)

include_directories(include)

add_library(customtype_extension STATIC customtype-extension.cpp)

# build_loadable_extension(customtype customtype-extension.cpp)
install(
  TARGETS customtype_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
