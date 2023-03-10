cmake_minimum_required(VERSION 2.8.12)

project(MACAddrExtension)

include_directories(include)

set(MACADDR_EXTENSION_FILES macaddr-extension.cpp macaddr_functions.cpp
                            macaddr.cpp)

add_library(macaddr_extension STATIC ${MACADDR_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(macaddr ${PARAMETERS} ${MACADDR_EXTENSION_FILES})

install(
  TARGETS macaddr_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
