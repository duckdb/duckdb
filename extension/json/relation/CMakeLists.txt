add_library_unity(duckdb_json_relation OBJECT read_json_relation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_json_relation>
    PARENT_SCOPE)
