add_library_unity(duckdb_storage_table OBJECT column_segment.cpp column_statistics.cpp segment_tree.cpp transient_segment.cpp version_chunk.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_table> PARENT_SCOPE)
