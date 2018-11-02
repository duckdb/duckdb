
add_subdirectory(expression_rules)
add_subdirectory(logical_rules)

add_library(duckdb_optimizer OBJECT optimizer.cpp rewriter.cpp rule.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer> PARENT_SCOPE)
