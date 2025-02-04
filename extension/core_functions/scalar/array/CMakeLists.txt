add_library_unity(duckdb_core_functions_array OBJECT array_functions.cpp
                  array_value.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_array>
    PARENT_SCOPE)
