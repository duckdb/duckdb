add_library_unity(
  duckdb_operator_aggregate
  OBJECT
  aggregate_object.cpp
  distinct_aggregate_data.cpp
  physical_hash_aggregate.cpp
  grouped_aggregate_data.cpp
  physical_perfecthash_aggregate.cpp
  physical_ungrouped_aggregate.cpp
  physical_window.cpp
  physical_streaming_window.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_aggregate>
    PARENT_SCOPE)
