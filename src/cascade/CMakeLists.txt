add_library_unity(duckdb_cascade OBJECT aggregate_pullup.cpp aggregate_pushdown.cpp apply_decorrelation.cpp
                  cascade_config.cpp cascade_optimizer.cpp
                  groupby_reorder.cpp local_aggregate.cpp
                  segment_apply.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_cascade>
    PARENT_SCOPE)
