add_library_unity(
  duckdb_core_functions_list
  OBJECT
  flatten.cpp
  list_transform.cpp
  range.cpp
  list_value.cpp
  list_filter.cpp
  list_has_any_or_all.cpp
  list_aggregates.cpp
  list_distance.cpp
  array_slice.cpp
  list_sort.cpp
  list_reduce.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_list>
    PARENT_SCOPE)
