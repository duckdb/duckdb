add_library_unity(
  duckdb_storage_table
  OBJECT
  base_statistics.cpp
  chunk_info.cpp
  column_segment.cpp
  morsel_info.cpp
  segment_tree.cpp
  segment_statistics.cpp
  string_statistics.cpp
  persistent_segment.cpp
  transient_segment.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_table>
    PARENT_SCOPE)
