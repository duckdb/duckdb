add_subdirectory(expression)
add_subdirectory(operator)

add_library_unity(duckdb_optimizer_statistics OBJECT propagate_bound_order.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_statistics>
    PARENT_SCOPE)
