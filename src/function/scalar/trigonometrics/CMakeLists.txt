add_library_unity(duckdb_func_trig
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
                     $<TARGET_OBJECTS:duckdb_func_trig> PARENT_SCOPE)
