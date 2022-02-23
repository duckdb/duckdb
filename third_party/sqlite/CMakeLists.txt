if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

include_directories(include)

add_library(duckdb_sqlite3 STATIC sqlite3.c)
TARGET_LINK_LIBRARIES(duckdb_sqlite3 pthread)

