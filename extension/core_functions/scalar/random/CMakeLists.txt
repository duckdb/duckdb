add_library_unity(duckdb_core_functions_random OBJECT random.cpp setseed.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_random>
    PARENT_SCOPE)
