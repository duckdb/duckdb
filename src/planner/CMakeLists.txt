
add_library(duckdb_planner OBJECT logicalfilter.cpp logicaloperators.cpp logicalplangenerator.cpp binder.cpp bindcontext.cpp planner.cpp logical_visitor.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner> PARENT_SCOPE)
