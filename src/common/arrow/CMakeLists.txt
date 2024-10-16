add_library_unity(
  duckdb_common_arrow
  OBJECT
  arrow_appender.cpp
  arrow_converter.cpp
  arrow_util.cpp
  arrow_merge_event.cpp
  arrow_query_result.cpp
  arrow_wrapper.cpp
  physical_arrow_collector.cpp
  physical_arrow_batch_collector.cpp
  schema_metadata.cpp)
add_subdirectory(appender)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_arrow>
    PARENT_SCOPE)
