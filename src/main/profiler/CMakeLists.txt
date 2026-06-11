add_library_unity(duckdb_main_profiler OBJECT gathered_metrics.cpp
                  metrics_manager.cpp profiling_utils.cpp query_profiler.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_profiler>
    PARENT_SCOPE)
