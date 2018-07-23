

add_library(duckdb_execution_vector OBJECT vector_operations.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_vector> PARENT_SCOPE)
