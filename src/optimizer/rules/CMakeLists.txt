
add_library(duckdb_optimizer_rules OBJECT constant_cast.cpp constant_folding.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_rules> PARENT_SCOPE)
