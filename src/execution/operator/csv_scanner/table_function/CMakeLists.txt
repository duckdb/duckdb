add_library_unity(duckdb_operator_csv_table_function OBJECT
                  global_csv_state.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_csv_table_function>
    PARENT_SCOPE)
