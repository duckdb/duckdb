include_directories(include)

add_library(duckdb_yyjson STATIC yyjson.cpp)

disable_target_warnings(duckdb_yyjson)

install(TARGETS duckdb_yyjson
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")