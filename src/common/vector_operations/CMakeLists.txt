add_library_unity(duckdb_vector_operations
                  OBJECT
                  append.cpp
                  boolean_operators.cpp
                  cast_operators.cpp
                  comparison_operators.cpp
                  comparison_select_operators.cpp
                  copy.cpp
                  generators.cpp
                  gather.cpp
                  hash.cpp
                  null_operations.cpp
                  numeric_inplace_operators.cpp
                  numeric_binary_operators.cpp
                  scatter.cpp
                  set.cpp
                  sort.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_vector_operations> PARENT_SCOPE)
