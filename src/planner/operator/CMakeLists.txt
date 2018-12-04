
add_library(duckdb_planner_operator OBJECT logical_aggregate.cpp logical_cross_product.cpp 
            logical_filter.cpp logical_get.cpp logical_join.cpp logical_projection.cpp 
            logical_prune_columns.cpp logical_table_function.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner_operator> PARENT_SCOPE)
