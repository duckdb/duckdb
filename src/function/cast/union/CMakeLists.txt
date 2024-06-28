add_library_unity(duckdb_union_cast OBJECT from_struct.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_union_cast>
    PARENT_SCOPE)
