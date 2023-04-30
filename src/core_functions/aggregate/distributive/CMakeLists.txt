add_library_unity(
  duckdb_aggr_distributive
  OBJECT
  approx_count.cpp
  arg_min_max.cpp
  bitagg.cpp
  bitstring_agg.cpp
  bool.cpp
  entropy.cpp
  kurtosis.cpp
  minmax.cpp
  product.cpp
  skew.cpp
  string_agg.cpp
  sum.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_distributive>
    PARENT_SCOPE)
