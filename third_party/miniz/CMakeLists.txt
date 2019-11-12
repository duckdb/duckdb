cmake_policy(SET CMP0063 NEW)

add_definitions(-DMINIZ_NO_ARCHIVE_APIS)
add_library(miniz STATIC miniz.c)

target_include_directories(miniz PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(miniz PROPERTIES
    EXPORT_NAME duckdb_miniz)

install(
    TARGETS miniz
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
