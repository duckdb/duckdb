cmake_policy(SET CMP0063 NEW)
add_library(hyperloglog STATIC hyperloglog.c sds.c)

target_include_directories(hyperloglog PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(hyperloglog PROPERTIES
    EXPORT_NAME duckdb_hyperloglog)
