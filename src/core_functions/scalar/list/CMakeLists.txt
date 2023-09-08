add_library_unity(
  duckdb_func_list
  OBJECT
  array_slice.cpp
  flatten.cpp
  list_aggregates.cpp
  list_filter.cpp
  list_sort.cpp
  list_distance.cpp
  list_cosine_similarity.cpp
  list_inner_product.cpp
  list_transform.cpp
  list_value.cpp
  range.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_list>
    PARENT_SCOPE)
