
add_library(duckdb_execution_operator OBJECT physical_aggregate.cpp physical_filter.cpp physical_limit.cpp physical_table_scan.cpp physical_projection.cpp physical_hash_aggregate.cpp physical_insert.cpp physical_order.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_operator> PARENT_SCOPE)
