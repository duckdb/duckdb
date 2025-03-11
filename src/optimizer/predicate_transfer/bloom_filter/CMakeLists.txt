add_library_unity(duckdb_bloom_filter OBJECT bloom_filter.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_bloom_filter>
    PARENT_SCOPE)
