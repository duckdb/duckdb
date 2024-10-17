add_library_unity(duckdb_function_map OBJECT map_contains.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_function_map>
    PARENT_SCOPE)
