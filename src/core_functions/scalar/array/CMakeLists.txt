add_library_unity(duckdb_func_array OBJECT array_value.cpp array_functions.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_array>
    PARENT_SCOPE)
