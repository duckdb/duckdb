
add_library(duckdb_value_operations OBJECT operations.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_value_operations> PARENT_SCOPE)
