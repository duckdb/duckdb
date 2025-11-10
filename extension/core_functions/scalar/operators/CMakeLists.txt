add_library_unity(duckdb_core_functions_operators OBJECT bitwise.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_operators>
    PARENT_SCOPE)
