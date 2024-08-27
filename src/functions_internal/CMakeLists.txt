add_subdirectory(scalar)

add_library_unity(duckdb_internal_functions OBJECT function_list.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_internal_functions>
    PARENT_SCOPE)
