add_library_unity(duckdb_func_pragma OBJECT pragma_functions.cpp
                  pragma_queries.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_pragma>
    PARENT_SCOPE)
