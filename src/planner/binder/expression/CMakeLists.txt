add_library_unity(duckdb_bind_expression
                  OBJECT
                  bind_aggregate_expression.cpp
                  bind_case_expression.cpp
                  bind_cast_expression.cpp
                  bind_collate_expression.cpp
                  bind_columnref_expression.cpp
                  bind_comparison_expression.cpp
                  bind_conjunction_expression.cpp
                  bind_constant_expression.cpp
                  bind_function_expression.cpp
                  bind_operator_expression.cpp
                  bind_parameter_expression.cpp
                  bind_subquery_expression.cpp
                  bind_unnest_expression.cpp
                  bind_window_expression.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_bind_expression> PARENT_SCOPE)
