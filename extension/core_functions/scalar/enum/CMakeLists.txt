add_library_unity(duckdb_core_functions_enum OBJECT enum_functions.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_enum>
    PARENT_SCOPE)
