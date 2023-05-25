add_library_unity(
  duckdb_operator_result_collector OBJECT physical_batch_collector.cpp
  physical_numpy_collector.cpp physical_materialized_collector.cpp
  physical_result_collector.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_result_collector>
    PARENT_SCOPE)
