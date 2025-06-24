add_library_unity(
  duckdb_core_functions_distributive
  OBJECT
  kurtosis.cpp
  string_agg.cpp
  sum.cpp
  arg_min_max.cpp
  approx_count.cpp
  skew.cpp
  bitagg.cpp
  bitstring_agg.cpp
  product.cpp
  bool.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_distributive>
    PARENT_SCOPE)
