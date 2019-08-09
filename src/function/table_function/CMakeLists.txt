add_subdirectory(sqlite)
add_library_unity(duckdb_table_function
                  OBJECT
                  sqlite_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_table_function>
    PARENT_SCOPE)
