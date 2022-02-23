add_library_unity(duckdb_func_uuid OBJECT gen_random.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_uuid>
    PARENT_SCOPE)
