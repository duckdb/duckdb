add_library_unity(
  duckdb_aggr_regr
  OBJECT
        regr_avg.cpp
        regr_count.cpp
regr_slope.cpp
regr_r2.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_regr>
    PARENT_SCOPE)
