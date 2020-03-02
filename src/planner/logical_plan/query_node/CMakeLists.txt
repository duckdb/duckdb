add_library_unity(duckdb_logical_plan_node
                  OBJECT
                  plan_query_node.cpp
                  plan_select_node.cpp
                  plan_setop.cpp
                  plan_recursive_cte_node.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_logical_plan_node> PARENT_SCOPE)
