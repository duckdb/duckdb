add_library_unity(duckdb_storage_table
                  OBJECT
                  column_segment.cpp
                  segment_tree.cpp
                  version_chunk.cpp
                  version_chunk_info.cpp
                  transient_segment.cpp
                  persistent_segment.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_table>
    PARENT_SCOPE)
