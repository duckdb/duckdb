
add_subdirectory(expression_rules)
add_subdirectory(join_order)
add_subdirectory(logical_rules)

add_library(duckdb_optimizer OBJECT join_order_optimizer.cpp optimizer.cpp rewriter.cpp rule.cpp subquery_rewriter.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer> PARENT_SCOPE)
