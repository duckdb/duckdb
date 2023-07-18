cmake_minimum_required(VERSION 2.8.12)

project(ExcelExtension)

include_directories(numformat/include)
include_directories(include)
add_subdirectory(numformat)

build_static_extension(excel excel_extension.cpp ${NUMFORMAT_OBJECT_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(excel ${PARAMETERS} excel_extension.cpp
                         ${NUMFORMAT_OBJECT_FILES})

install(
  TARGETS excel_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
