add_library_unity(duckdb_core_functions_debug OBJECT vector_type.cpp sleep.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_debug>
    PARENT_SCOPE)
