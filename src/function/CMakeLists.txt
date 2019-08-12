add_subdirectory(aggregate)
add_subdirectory(scalar)
add_subdirectory(table)

add_library(duckdb_function OBJECT function.cpp function_set.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_function>
    PARENT_SCOPE)
