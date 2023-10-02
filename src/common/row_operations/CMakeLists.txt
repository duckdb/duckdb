add_library_unity(
  duckdb_row_operations
  OBJECT
  row_aggregate.cpp
  row_scatter.cpp
  row_gather.cpp
  row_matcher.cpp
  row_external.cpp
  row_radix_scatter.cpp
  row_heap_scatter.cpp
  row_heap_gather.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_row_operations>
    PARENT_SCOPE)
