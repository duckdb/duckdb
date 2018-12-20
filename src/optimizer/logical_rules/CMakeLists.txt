
add_library(duckdb_logical_rules OBJECT remove_obsolete_filters.cpp split_filter_conjunctions.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_rules> PARENT_SCOPE)