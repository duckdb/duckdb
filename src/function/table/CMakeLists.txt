add_subdirectory(sqlite)
add_library_unity(duckdb_func_table
                  OBJECT
                  copy_csv.cpp
                  read_csv.cpp
                  sqlite_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_table>
    PARENT_SCOPE)
