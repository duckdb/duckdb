add_subdirectory(bit)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_bit>
    PARENT_SCOPE)
