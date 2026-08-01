cmake_minimum_required(VERSION 2.8.12...3.29)

project(ICUExtension)

# Before any subdirectory or target, which would otherwise inherit the
# directory-wide definition
set_extension_version_sources(icu icu_extension.cpp)

include_directories(include)
include_directories(collation/include)
include_directories(datetime/include)
option(WITH_INTERNAL_ICU "Use vendored copy of icu" TRUE)
if(WITH_INTERNAL_ICU)
  include_directories(third_party/icu/common)
  include_directories(third_party/icu/i18n)

  add_subdirectory(third_party)
endif()

set(ICU_EXTENSION_FILES
    ${ICU_LIBRARY_FILES}
    collation/collation_collator.cpp
    collation/collation_normalizer.cpp
    collation/generated/collation_root_data.cpp
    collation/generated/collation_normalization_data.cpp
    collation/generated/collation_tailoring_data.cpp
    datetime/grego.cpp
    datetime/timezone.cpp
    datetime/tz_reference.cpp
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
if(NOT WITH_INTERNAL_ICU)
  find_package(
    ICU
    COMPONENTS i18n uc data
    REQUIRED)
  target_link_libraries(icu_extension ICU::i18n ICU::uc ICU::data)
endif()
disable_target_warnings(icu_extension)
set(PARAMETERS "-no-warnings")
build_loadable_extension(icu ${PARAMETERS} ${ICU_EXTENSION_FILES})
if(NOT WITH_INTERNAL_ICU)
  target_link_libraries(icu_loadable_extension ICU::i18n ICU::uc ICU::data)
endif()
install(
  TARGETS icu_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
