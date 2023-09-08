add_library_unity(
  duckdb_func_list
  OBJECT
  array_slice.cpp
  flatten.cpp
  list_aggregates.cpp
  list_lambdas.cpp
  list_value.cpp
  list_sort.cpp
  list_distance.cpp
  list_cosine_similarity.cpp
  list_inner_product.cpp
  range.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_list>
    PARENT_SCOPE)
