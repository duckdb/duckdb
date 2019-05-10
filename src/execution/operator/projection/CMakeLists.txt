add_library_unity(duckdb_operator_projection OBJECT physical_projection.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_projection> PARENT_SCOPE)
