add_library_unity(duckdb_scalar_function_date
				  OBJECT
				  age.cpp
				  date_part.cpp
				  year.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_scalar_function_date> PARENT_SCOPE)
