
add_library(duckdb_planner_operator OBJECT logical_filter.cpp logical_join.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner_operator> PARENT_SCOPE)
