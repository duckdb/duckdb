add_library_unity(duckdb_operator_set OBJECT physical_union.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_set> PARENT_SCOPE)
