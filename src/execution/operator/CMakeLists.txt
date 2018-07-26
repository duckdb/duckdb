
add_library(duckdb_execution_operator OBJECT physical_filter.cpp physical_limit.cpp physical_seq_scan.cpp physical_projection.cpp physical_hash_aggregate.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_operator> PARENT_SCOPE)
