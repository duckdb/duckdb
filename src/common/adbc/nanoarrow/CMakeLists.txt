add_library_unity(duckdb_adbc_nanoarrow OBJECT metadata.cpp schema.cpp
                  allocator.cpp single_batch_array_stream.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_adbc_nanoarrow>
    PARENT_SCOPE)
