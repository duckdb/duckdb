cmake_minimum_required(VERSION 2.8.12...3.29)

project(ICUExtension)

# Before any subdirectory or target, which would otherwise inherit the
# directory-wide definition
set_extension_version_sources(icu icu_extension.cpp)

include_directories(include)
include_directories(collation/include)
include_directories(datetime/include)

set(ICU_EXTENSION_FILES
    collation/collation_collator.cpp
    collation/collation_normalizer.cpp
    collation/generated/collation_root_data.cpp
    collation/generated/collation_normalization_data.cpp
    collation/generated/collation_tailoring_data.cpp
    datetime/astronomy.cpp
    datetime/calendar.cpp
    datetime/chinese.cpp
    datetime/coptic.cpp
    datetime/grego.cpp
    datetime/gregorian.cpp
    datetime/hebrew.cpp
    datetime/islamic.cpp
    datetime/japanese.cpp
    datetime/persian.cpp
    datetime/timezone.cpp
    datetime/generated/tz_data.cpp
    icu_extension.cpp
    icu-current.cpp
    icu-dateadd.cpp
    icu-datefunc.cpp
    icu-datepart.cpp
    icu-datesub.cpp
    icu-datetrunc.cpp
    icu-makedate.cpp
    icu-list-range.cpp
    icu-table-range.cpp
    icu-strptime.cpp
    icu-timebucket.cpp
    icu-timezone.cpp)

build_static_extension(icu ${ICU_EXTENSION_FILES})
link_threads(icu_extension "")
disable_target_warnings(icu_extension)
set(PARAMETERS "-no-warnings")
build_loadable_extension(icu ${PARAMETERS} ${ICU_EXTENSION_FILES})
install(
  TARGETS icu_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
