
add_library(duckdb_planner OBJECT bindcontext.cpp binder.cpp planner.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner> PARENT_SCOPE)
