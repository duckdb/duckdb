cmake_minimum_required(VERSION 2.8.12)

project(INETExtension)

include_directories(include)

set(INET_EXTENSION_FILES inet_extension.cpp inet_functions.cpp ipaddress.cpp)

add_library(inet_extension STATIC ${INET_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(inet ${PARAMETERS} ${INET_EXTENSION_FILES})

install(
  TARGETS inet_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
