cmake_policy(SET CMP0063 NEW)
add_library(hyperloglog STATIC hyperloglog.cpp sds.cpp)

target_include_directories(
  hyperloglog
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(hyperloglog PROPERTIES EXPORT_NAME duckdb_hyperloglog)

install(TARGETS hyperloglog
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(hyperloglog)
