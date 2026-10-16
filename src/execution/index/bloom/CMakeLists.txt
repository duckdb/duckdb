add_library_unity(duckdb_execution_index_bloom OBJECT bloom_index.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution_index_bloom>
    PARENT_SCOPE)
