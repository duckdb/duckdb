add_library_unity(duckdb_func_enum OBJECT enum_first.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_enum>
    PARENT_SCOPE)
