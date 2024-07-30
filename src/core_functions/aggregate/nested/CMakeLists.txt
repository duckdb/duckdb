add_library_unity(duckdb_aggr_nested OBJECT binned_histogram.cpp list.cpp
                  histogram.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_nested>
    PARENT_SCOPE)
