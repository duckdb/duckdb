cmake_policy(SET CMP0063 NEW)
add_library(blockquicksort STATIC blockquicksort_wrapper.cpp)

include_directories (${CMAKE_BINARY_DIR})

target_include_directories(
  blockquicksort
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(blockquicksort PROPERTIES EXPORT_NAME duckdb_blockquicksort)

install(TARGETS blockquicksort
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(blockquicksort)
