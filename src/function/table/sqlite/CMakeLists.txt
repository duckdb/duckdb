add_library_unity(duckdb_func_sqlite
                  OBJECT
                  pragma_collations.cpp
                  pragma_table_info.cpp
                  sqlite_master.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_sqlite>
    PARENT_SCOPE)
