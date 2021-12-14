cmake_minimum_required(VERSION 2.8.12)

project(ICUExtension)

include_directories(include)

set(ICU_EXTENSION_FILES
    icu-collate.cpp
    icu-extension.cpp
    icu-dateadd.cpp
    icu-datefunc.cpp
    icu-datepart.cpp
    icu-datesub.cpp
    icu-datetrunc.cpp)

add_library(icu_extension STATIC ${ICU_EXTENSION_FILES})
link_threads(icu_extension)
disable_target_warnings(icu_extension)

build_loadable_extension(icu ${ICU_EXTENSION_FILES})
install(
  TARGETS icu_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
