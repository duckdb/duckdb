cmake_minimum_required(VERSION 2.8.12)

project(EXCELExtension)

include_directories(numformat/include)
include_directories(include)
add_subdirectory(numformat)

add_library(excel_extension STATIC excel-extension.cpp
                                   ${NUMFORMAT_OBJECT_FILES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(excel excel-extension.cpp ${NUMFORMAT_OBJECT_FILES}
                         ${IGNORE_WARNINGS})

install(
  TARGETS excel_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
