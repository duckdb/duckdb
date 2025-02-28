add_subdirectory(bloom_filter)

add_library_unity(
  duckdb_predicate_transfer
  OBJECT
  transfer_graph_manager.cpp
  dag.cpp
  predicate_transfer_optimizer.cpp
  table_operator_namager.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_predicate_transfer>
    PARENT_SCOPE)
