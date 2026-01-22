add_library_unity(duckdb_function_variant OBJECT variant_shredding.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_function_variant>
    PARENT_SCOPE)
