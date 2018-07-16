
add_library(duckdb_transform OBJECT transform_expression.cpp transform_statement.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_transform> PARENT_SCOPE)
