add_library_unity(
  duckdb_query_node
  OBJECT
  recursive_cte_node.cpp
  cte_node.cpp
  select_node.cpp
  set_operation_node.cpp
  statement_node.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_query_node>
    PARENT_SCOPE)
