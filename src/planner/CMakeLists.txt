add_subdirectory(expression)
add_subdirectory(binder)
add_subdirectory(expression_binder)
add_subdirectory(operator)
add_subdirectory(subquery)

add_library_unity(duckdb_planner
                  OBJECT
                  expression_iterator.cpp
                  expression.cpp
                  table_binding.cpp
                  expression_binder.cpp
                  joinside.cpp
                  logical_operator.cpp
                  pragma_handler.cpp
                  binder.cpp
                  bind_context.cpp
                  planner.cpp
                  logical_operator_visitor.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner>
    PARENT_SCOPE)
