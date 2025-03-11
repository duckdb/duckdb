add_library_unity(
  duckdb_bloom_filter OBJECT bloom_filter_avx2.cpp bloom_filter.cpp partition_util.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_bloom_filter>
    PARENT_SCOPE)
