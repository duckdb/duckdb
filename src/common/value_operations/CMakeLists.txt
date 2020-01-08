add_library_unity(duckdb_value_operations
                  OBJECT
                  comparison_operations.cpp
                  hash.cpp
                  numeric_operations.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_value_operations> PARENT_SCOPE)
