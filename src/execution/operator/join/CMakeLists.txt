add_library(duckdb_operator_join OBJECT physical_cross_product.cpp physical_hash_join.cpp physical_join.cpp physical_nested_loop_join.cpp physical_piecewise_merge_join.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_join> PARENT_SCOPE)
