add_library_unity(duckdb_main_capi_cast OBJECT utils-c.cpp from_decimal-c.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_capi_cast>
    PARENT_SCOPE)
