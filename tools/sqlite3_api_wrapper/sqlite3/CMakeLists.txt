# this directory contains some of the original code from SQLite mainly helper
# functions that we don't really care about replacing ourselves

add_definitions(-DUSE_DUCKDB_SHELL_WRAPPER)
set(SQLITE_FILES printf.c strglob.c)
if(WIN32)
  set(SQLITE_FILES ${SQLITE_FILES} os_win.c)
endif()

add_library(sqlite3_api_wrapper_sqlite3 OBJECT ${SQLITE_FILES})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:sqlite3_api_wrapper_sqlite3>
    PARENT_SCOPE)
