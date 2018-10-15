add_library(duckdb_expression OBJECT aggregate_expression.cpp case_expression.cpp cast_expression.cpp conjunction_expression.cpp columnref_expression.cpp comparison_expression.cpp constant_expression.cpp operator_expression.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_expression> PARENT_SCOPE)
