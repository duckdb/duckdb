add_subdirectory(expression)
add_subdirectory(statement)
add_subdirectory(transform)

add_library(duckdb_parser OBJECT expression.cpp parser.cpp sql_node_visitor.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser> PARENT_SCOPE)
