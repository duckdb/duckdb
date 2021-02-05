add_library_unity(
  duckdb_aggr_distr
  OBJECT
        approx_count.cpp
  bitagg.cpp
  count.cpp
  first.cpp
  minmax.cpp
  string_agg.cpp
  sum.cpp
  product.cpp
bool.cpp
regression.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_distr>
    PARENT_SCOPE)
