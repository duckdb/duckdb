
add_library(duckdb_logical_planner OBJECT operators.cpp plangenerator.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_planner> PARENT_SCOPE)
