
add_library(duckdb_expression_rules OBJECT constant_cast.cpp constant_folding.cpp distributivity.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_expression_rules> PARENT_SCOPE)