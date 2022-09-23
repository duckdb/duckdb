add_library_unity(
  duckdb_func_list
  OBJECT
  list_lambdas.cpp
  list_concat.cpp
  contains_or_position.cpp
  list_aggregates.cpp
  array_slice.cpp
  list_extract.cpp
  list_sort.cpp
  list_value.cpp
  range.cpp
  flatten.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_list>
    PARENT_SCOPE)
