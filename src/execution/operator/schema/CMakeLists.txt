add_library_unity(duckdb_operator_schema
                  OBJECT
                  physical_create_index.cpp
                  physical_create_table.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_operator_schema> PARENT_SCOPE)
