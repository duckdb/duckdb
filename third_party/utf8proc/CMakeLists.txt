if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

include_directories(include)

add_library(duckdb_utf8proc STATIC utf8proc.cpp utf8proc_wrapper.cpp)


install(TARGETS duckdb_utf8proc
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
