add_subdirectory(aggregate)
add_subdirectory(scalar)

add_library_unity(duckdb_core_functions OBJECT core_functions.cpp
                  function_list.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_core_functions>
    PARENT_SCOPE)
