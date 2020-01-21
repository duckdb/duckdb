add_subdirectory(query_node)
add_subdirectory(statement)
add_subdirectory(tableref)

add_library(duckdb_logical_plan OBJECT plan_subquery.cpp query_node/plan_recursive_cte_node.cpp tableref/plan_cteref.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_plan>
    PARENT_SCOPE)
