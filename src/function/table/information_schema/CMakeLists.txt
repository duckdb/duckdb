add_library_unity(duckdb_func_information_schema OBJECT information_schema_schemata.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_information_schema>
    PARENT_SCOPE)
