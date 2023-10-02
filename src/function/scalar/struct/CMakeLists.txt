add_library_unity(duckdb_func_struct_main OBJECT struct_extract.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_struct_main>
    PARENT_SCOPE)
