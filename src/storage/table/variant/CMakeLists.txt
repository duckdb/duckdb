add_library_unity(duckdb_storage_table_variant OBJECT variant_shredding.cpp
                  variant_unshredding.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_table_variant>
    PARENT_SCOPE)
