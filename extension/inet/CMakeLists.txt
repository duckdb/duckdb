cmake_minimum_required(VERSION 2.8.12)

project(INETExtension)

include_directories(include)

set(INET_EXTENSION_FILES inet-extension.cpp inet_functions.cpp ipaddress.cpp)

add_library(inet_extension STATIC ${INET_EXTENSION_FILES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(json ${INET_EXTENSION_FILES} ${IGNORE_WARNINGS})

install(
  TARGETS inet_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
