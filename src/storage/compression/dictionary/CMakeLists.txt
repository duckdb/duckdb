add_library_unity(duckdb_storage_compression_dictionary OBJECT common.cpp
                  decompression.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression_dictionary>
    PARENT_SCOPE)
