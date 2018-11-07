
add_subdirectory(operator)

add_library(duckdb_planner OBJECT logical_operator.cpp logical_plan_generator.cpp binder.cpp bindcontext.cpp planner.cpp logical_operator_visitor.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner> PARENT_SCOPE)
