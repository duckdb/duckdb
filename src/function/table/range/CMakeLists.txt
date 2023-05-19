add_library_unity(duckdb_func_table_range OBJECT int_range_executor.cpp
                  time_range_executor.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_table_range>
    PARENT_SCOPE)
