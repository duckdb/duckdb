cmake_policy(SET CMP0063 NEW)
add_library(fmt STATIC format.cc)

target_include_directories(fmt PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(fmt PROPERTIES EXPORT_NAME duckdb_fmt)

install(TARGETS fmt
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(fmt)
