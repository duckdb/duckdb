
add_library(duckdb_logical_rules OBJECT cross_product_rewrite.cpp exists_rewrite.cpp in_clause_rewrite.cpp remove_obsolete_filters.cpp selection_pushdown.cpp split_filter_conjunctions.cpp subquery_rewrite.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_rules> PARENT_SCOPE)