add_library_unity(duckdb_aggr_algebraic OBJECT avg.cpp covar.cpp stddev.cpp
                  corr.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_algebraic>
    PARENT_SCOPE)
