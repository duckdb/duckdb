add_library_unity(duckdb_core_functions_math OBJECT numeric.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_math>
    PARENT_SCOPE)
