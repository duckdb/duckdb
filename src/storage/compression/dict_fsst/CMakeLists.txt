add_library_unity(duckdb_storage_compression_dict_fsst OBJECT common.cpp
                  analyze.cpp compression.cpp decompression.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression_dict_fsst>
    PARENT_SCOPE)
