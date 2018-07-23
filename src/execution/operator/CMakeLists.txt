
add_library(duckdb_execution_operator OBJECT physical_limit.cpp physical_seq_scan.cpp physical_projection.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_operator> PARENT_SCOPE)
