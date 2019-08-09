add_library_unity(duckdb_aggregate_function_algebraic
				OBJECT
				avg.cpp
				covar.cpp
				stddev.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_aggregate_function_algebraic> PARENT_SCOPE)
