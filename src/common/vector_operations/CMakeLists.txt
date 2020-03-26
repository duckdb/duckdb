add_library_unity(duckdb_vector_operations
                  OBJECT
                  boolean_operators.cpp
                  vector_cast.cpp
                  comparison_operators.cpp
                  vector_copy.cpp
                  generators.cpp
                  gather.cpp
                  vector_hash.cpp
                  vector_storage.cpp
                  null_operations.cpp
                  numeric_inplace_operators.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_vector_operations> PARENT_SCOPE)
