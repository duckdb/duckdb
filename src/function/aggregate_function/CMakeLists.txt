add_library_unity(duckdb_aggregate_function
                  OBJECT
                  distributive.cpp
                  algebraic.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_aggregate_function> PARENT_SCOPE)
