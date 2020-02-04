add_library_unity(duckdb_bind_query_node
                  OBJECT
                  bind_select_node.cpp
                  bind_setop_node.cpp
                  bind_recursive_cte_node.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_bind_query_node> PARENT_SCOPE)
