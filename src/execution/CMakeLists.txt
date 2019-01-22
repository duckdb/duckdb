
add_subdirectory(expression_executor)
add_subdirectory(merge_join)
add_subdirectory(operator)

add_library(duckdb_execution OBJECT aggregate_hashtable.cpp column_binding_resolver.cpp executor.cpp expression_executor.cpp join_hashtable.cpp ../storage/order_index.cpp physical_operator.cpp physical_plan_generator.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution> PARENT_SCOPE)
