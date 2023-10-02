add_library_unity(
  duckdb_aggr_regr
  OBJECT
  regr_avg.cpp
  regr_count.cpp
  regr_slope.cpp
  regr_r2.cpp
  regr_sxx_syy.cpp
  regr_sxy.cpp
  regr_intercept.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_regr>
    PARENT_SCOPE)
