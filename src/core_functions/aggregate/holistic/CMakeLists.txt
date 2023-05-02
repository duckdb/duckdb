add_library_unity(duckdb_aggr_holistic OBJECT quantile.cpp mode.cpp
                  approximate_quantile.cpp reservoir_quantile.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_holistic>
    PARENT_SCOPE)
