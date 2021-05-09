add_library_unity(duckdb_func_seq OBJECT nextval.cpp currval.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_seq>
    PARENT_SCOPE)
