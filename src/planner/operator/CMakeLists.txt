add_library_unity(duckdb_planner_operator
                  OBJECT
                  logical_aggregate.cpp
                  logical_empty_result.cpp
                  logical_any_join.cpp
                  logical_comparison_join.cpp
                  logical_cross_product.cpp
                  logical_filter.cpp
                  logical_get.cpp
                  logical_join.cpp
                  logical_projection.cpp
                  logical_table_function.cpp
                  logical_unnest.cpp
                  logical_window.cpp
                  logical_distinct.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_planner_operator> PARENT_SCOPE)
