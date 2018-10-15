
add_subdirectory(constraints)
add_subdirectory(expression)
add_subdirectory(statement)
add_subdirectory(tableref)
add_subdirectory(transform)

add_library(duckdb_parser OBJECT constraint.cpp expression.cpp parser.cpp sql_node_visitor.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser> PARENT_SCOPE)
