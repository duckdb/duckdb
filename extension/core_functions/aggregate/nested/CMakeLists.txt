add_library_unity(duckdb_core_functions_nested OBJECT binned_histogram.cpp
                  list.cpp histogram.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_nested>
    PARENT_SCOPE)
