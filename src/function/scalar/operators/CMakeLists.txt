add_library_unity(duckdb_func_ops
				OBJECT
				add.cpp
				subtract.cpp
				multiply.cpp
				divide.cpp
				mod.cpp
				left_shift.cpp
				right_shift.cpp
				bitwise_and.cpp
				bitwise_or.cpp
				bitwise_xor.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_func_ops> PARENT_SCOPE)
