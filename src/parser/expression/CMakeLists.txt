add_library(duckdb_expression OBJECT abstract_expression.cpp aggregate_expression.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_expression> PARENT_SCOPE)
