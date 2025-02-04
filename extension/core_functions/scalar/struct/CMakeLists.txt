add_library_unity(duckdb_core_functions_struct OBJECT struct_insert.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_struct>
    PARENT_SCOPE)
