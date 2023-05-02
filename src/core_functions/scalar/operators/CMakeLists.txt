add_library_unity(duckdb_func_ops OBJECT bitwise.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_ops>
    PARENT_SCOPE)
