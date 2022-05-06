cmake_minimum_required(VERSION 2.8.12)

project(ICUExtension)

include_directories(include)
include_directories(third_party/icu/common)
include_directories(third_party/icu/i18n)

add_subdirectory(third_party)

set(ICU_EXTENSION_FILES
    ${ICU_LIBRARY_FILES}
    icu-extension.cpp
    icu-dateadd.cpp
    icu-datefunc.cpp
    icu-datepart.cpp
    icu-datesub.cpp
    icu-datetrunc.cpp
    icu-makedate.cpp
    icu-strptime.cpp)

add_library(icu_extension STATIC ${ICU_EXTENSION_FILES})
link_threads(icu_extension)
disable_target_warnings(icu_extension)

build_loadable_extension(icu ${ICU_EXTENSION_FILES})
install(
  TARGETS icu_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
