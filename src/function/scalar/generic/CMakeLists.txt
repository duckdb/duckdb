add_library_unity(duckdb_func_generic OBJECT alias.cpp constant_or_null.cpp least.cpp null_if.cpp stats.cpp typeof.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_generic>
    PARENT_SCOPE)
