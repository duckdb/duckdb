add_library_unity(duckdb_func_debug_main OBJECT vector_type.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_debug_main>
    PARENT_SCOPE)
