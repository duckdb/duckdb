add_library_unity(
  duckdb_parallel
  OBJECT
  base_pipeline_event.cpp
  executor_task.cpp
  executor.cpp
  event.cpp
  pipeline.cpp
  pipeline_complete_event.cpp
  pipeline_event.cpp
  pipeline_executor.cpp
  pipeline_finish_event.cpp
  task_scheduler.cpp
  thread_context.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parallel>
    PARENT_SCOPE)
