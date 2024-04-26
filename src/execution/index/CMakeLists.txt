add_subdirectory(art)
add_library_unity(
  duckdb_execution_index
  OBJECT
  fixed_size_allocator.cpp
  fixed_size_buffer.cpp
  unbound_index.cpp
  index_type_set.cpp
  bound_index.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_index>
    PARENT_SCOPE)
