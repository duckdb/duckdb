add_library_unity(duckdb_core_functions_blob OBJECT base64.cpp encode.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_blob>
    PARENT_SCOPE)
