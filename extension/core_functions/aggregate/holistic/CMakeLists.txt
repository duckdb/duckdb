add_library_unity(
  duckdb_core_functions_holistic
  OBJECT
  approx_top_k.cpp
  quantile.cpp
  reservoir_quantile.cpp
  mad.cpp
  approximate_quantile.cpp
  mode.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_holistic>
    PARENT_SCOPE)
