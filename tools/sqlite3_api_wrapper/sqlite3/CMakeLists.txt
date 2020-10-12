
# this directory contains some of the original code from SQLite
# mainly helper functions that we don't really care about replacing ourselves

add_library_unity(sqlite3_api_wrapper_sqlite3 OBJECT printf.c os_win.c strglob.c)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:sqlite3_api_wrapper_sqlite3>
    PARENT_SCOPE)
