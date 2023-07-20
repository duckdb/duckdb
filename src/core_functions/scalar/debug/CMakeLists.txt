add_library_unity(duckdb_func_debug OBJECT vector_type.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_debug>
    PARENT_SCOPE)
