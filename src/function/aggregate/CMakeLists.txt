add_subdirectory(algebraic)
add_subdirectory(distributive)
add_subdirectory(nested)

add_library_unity(duckdb_func_aggr
                  OBJECT
                  algebraic_functions.cpp
                  distributive_functions.cpp
                  nested_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_aggr>
    PARENT_SCOPE)
