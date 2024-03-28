add_library_unity(duckdb_arrow_conversion OBJECT arrow_duck_schema.cpp
                  arrow_array_scan_state.cpp arrow_type_info.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_arrow_conversion>
    PARENT_SCOPE)
