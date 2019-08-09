add_library_unity(duckdb_scalar_function_sequence
				OBJECT
				nextval.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_scalar_function_sequence> PARENT_SCOPE)
