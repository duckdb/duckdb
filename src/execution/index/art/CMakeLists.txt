add_library_unity(
  duckdb_execution_index_art
  OBJECT
  art_key.cpp
  node.cpp
  iterator.cpp
  leaf.cpp
  base_node.cpp
  node48.cpp
  node256.cpp
  prefix.cpp
  base_leaf.cpp
  node256_leaf.cpp
  art.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_index_art>
    PARENT_SCOPE)
