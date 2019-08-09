add_library_unity(duckdb_aggregate_function_distributive
				OBJECT
				count_star.cpp
				count.cpp
				first.cpp
				max.cpp
				min.cpp
				string_agg.cpp
				sum.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_aggregate_function_distributive> PARENT_SCOPE)
