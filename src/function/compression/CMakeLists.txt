add_library_unity(duckdb_func_compression OBJECT rle.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_compression>
    PARENT_SCOPE)
