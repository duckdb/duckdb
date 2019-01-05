
add_subdirectory(join_order)
add_subdirectory(rules)

add_library(duckdb_optimizer OBJECT join_order_optimizer.cpp optimizer.cpp expression_rewriter.cpp subquery_rewriter.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer> PARENT_SCOPE)
