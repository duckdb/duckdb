add_library_unity(duckdb_common_json OBJECT json_value.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_json>
    PARENT_SCOPE)
