add_subdirectory(distributive)

add_library_unity(duckdb_func_aggr OBJECT distributive_functions.cpp
                  sorted_aggregate_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_aggr>
    PARENT_SCOPE)
