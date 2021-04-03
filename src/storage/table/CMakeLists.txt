add_library_unity(
  duckdb_storage_table
  OBJECT
  chunk_info.cpp
  column_segment.cpp
  morsel_info.cpp
  update_segment.cpp
  persistent_table_data.cpp
  segment_tree.cpp
  persistent_segment.cpp
  standard_column_data.cpp
  transient_segment.cpp
  validity_column_data.cpp
  validity_segment.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_table>
    PARENT_SCOPE)
