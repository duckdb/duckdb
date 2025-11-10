add_library_unity(
  duckdb_core_functions_regression
  OBJECT
  regr_sxy.cpp
  regr_intercept.cpp
  regr_count.cpp
  regr_r2.cpp
  regr_avg.cpp
  regr_slope.cpp
  regr_sxx_syy.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_regression>
    PARENT_SCOPE)
