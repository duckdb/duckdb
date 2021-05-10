add_library_unity(duckdb_parallel OBJECT executor.cpp pipeline.cpp
                  task_scheduler.cpp thread_context.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parallel>
    PARENT_SCOPE)
