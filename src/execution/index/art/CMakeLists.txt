add_library_unity(
  duckdb_execution_index_art
  OBJECT
  art_builder.cpp
  art.cpp
  art_key.cpp
  art_merger.cpp
  base_leaf.cpp
  base_node.cpp
  iterator.cpp
  leaf.cpp
  node.cpp
  node256.cpp
  node256_leaf.cpp
  node48.cpp
  plan_art.cpp
  prefix.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_index_art>
    PARENT_SCOPE)
