add_library_unity(duckdb_storage_table
                  OBJECT
                  chunk_info.cpp
                  column_segment.cpp
                  segment_tree.cpp
                  persistent_segment.cpp
                  transient_segment.cpp
                  version_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_table>
    PARENT_SCOPE)
