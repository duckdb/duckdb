add_subdirectory(expression)
add_subdirectory(binder)
add_subdirectory(expression_binder)
add_subdirectory(filter)
add_subdirectory(operator)
add_subdirectory(subquery)

add_library_unity(
  duckdb_planner
  OBJECT
  bound_result_modifier.cpp
  bound_parameter_map.cpp
  collation_binding.cpp
  expression_iterator.cpp
  expression.cpp
  table_binding.cpp
  expression_binder.cpp
  joinside.cpp
  logical_operator.cpp
  binder.cpp
  bind_context.cpp
  planner.cpp
  pragma_handler.cpp
  logical_operator_visitor.cpp
  table_filter.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner>
    PARENT_SCOPE)
