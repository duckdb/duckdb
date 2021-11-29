cmake_minimum_required(VERSION 2.8.12)

project(ICUExtension)

include_directories(include)

add_library(icu_extension STATIC icu-collate.cpp icu-extension.cpp
                                 icu-datepart.cpp icu-datetrunc.cpp)
link_threads(icu_extension)
disable_target_warnings(icu_extension)

install(
  TARGETS icu_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
