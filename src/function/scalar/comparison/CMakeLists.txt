add_library_unity(duckdb_func_comparison OBJECT between.cpp comparison.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_comparison>
    PARENT_SCOPE)
