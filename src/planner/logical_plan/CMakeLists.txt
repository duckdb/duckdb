add_subdirectory(query_node)
add_subdirectory(statement)
add_subdirectory(tableref)

add_library_unity(duckdb_logical_plan OBJECT plan_subquery.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_plan>
    PARENT_SCOPE)
