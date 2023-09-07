cmake_minimum_required(VERSION 2.8.12)

project(PlaceholderExtension)

set(PLACEHOLDER_EXTENSION_FILES placeholder_extension.cpp)

build_static_extension(placeholder ${PLACEHOLDER_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(placeholder ${PARAMETERS}
                         ${PLACEHOLDER_EXTENSION_FILES})

install(
  TARGETS placeholder_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
