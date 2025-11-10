add_library_unity(duckdb_core_functions_algebraic OBJECT corr.cpp stddev.cpp
                  avg.cpp covar.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_algebraic>
    PARENT_SCOPE)
