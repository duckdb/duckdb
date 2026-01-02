add_library_unity(duckdb_common_variant OBJECT variant.cpp variant_value.cpp
                  variant_value_convert.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_variant>
    PARENT_SCOPE)
