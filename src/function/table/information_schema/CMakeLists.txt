add_library_unity(
  duckdb_func_information_schema OBJECT
  information_schema_tables.cpp information_schema_columns.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_information_schema>
    PARENT_SCOPE)
