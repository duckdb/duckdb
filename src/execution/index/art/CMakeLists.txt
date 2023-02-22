add_library_unity(
  duckdb_art_index_execution
  OBJECT
  art_key.cpp
  art_node.cpp
  fixed_size_allocator.cpp
  iterator.cpp
  leaf.cpp
  node4.cpp
  node16.cpp
  node48.cpp
  node256.cpp
  swizzleable_pointer.cpp
  prefix.cpp
  art.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_art_index_execution>
    PARENT_SCOPE)
