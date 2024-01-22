add_library_unity(duckdb_storage_compression_alp OBJECT alp_constants.cpp
                  alp.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression_alp>
    PARENT_SCOPE)
