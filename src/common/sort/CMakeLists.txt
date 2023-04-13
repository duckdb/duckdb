add_library_unity(
  duckdb_sort
  OBJECT
  comparators.cpp
  merge_sorter.cpp
  partition_state.cpp
  radix_sort.cpp
  sort_state.cpp
  sorted_block.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_sort>
    PARENT_SCOPE)
