add_library_unity(duckdb_func_secret OBJECT which_secret.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_secret>
    PARENT_SCOPE)
