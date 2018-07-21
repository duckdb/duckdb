
add_library(duckdb_execution_operator OBJECT limit.cpp seqscan.cpp physical_projection.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_operator> PARENT_SCOPE)
