
add_subdirectory(operator)

add_library(duckdb_execution OBJECT aggregate_hashtable.cpp executor.cpp expression_executor.cpp physical_operator.cpp physical_plan_generator.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution> PARENT_SCOPE)
