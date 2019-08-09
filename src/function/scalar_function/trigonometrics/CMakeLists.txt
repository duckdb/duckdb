add_library_unity(duckdb_scalar_function_trigonometrics
				  OBJECT
				  acos.cpp
				  asin.cpp
				  atan.cpp
				  atan2.cpp
				  cos.cpp
				  cot.cpp
				  sin.cpp
				  tan.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_scalar_function_trigonometrics> PARENT_SCOPE)
