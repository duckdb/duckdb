add_library_unity(duckdb_planner_parsed_data OBJECT bound_create_table_info.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner_parsed_data>
    PARENT_SCOPE)
