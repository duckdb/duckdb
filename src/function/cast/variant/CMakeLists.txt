add_library_unity(duckdb_variant_cast OBJECT from_variant.cpp to_variant.cpp
                  to_json.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_variant_cast>
    PARENT_SCOPE)
