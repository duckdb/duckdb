
add_library(duckdb_logical_plan OBJECT plan_copy.cpp plan_create_index.cpp plan_create_table.cpp plan_delete.cpp plan_execute.cpp plan_insert.cpp plan_query_node.cpp plan_select.cpp plan_select_node.cpp plan_setop.cpp plan_update.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_plan> PARENT_SCOPE)
