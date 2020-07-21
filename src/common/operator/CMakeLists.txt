add_library_unity(duckdb_common_operators OBJECT cast_operators.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_common_operators> PARENT_SCOPE)
