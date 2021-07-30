add_library_unity(
  duckdb_storage_segment
  OBJECT
  compressed_segment.cpp
  constant_segment.cpp
  numeric_uncompressed.cpp
  string_segment.cpp
  validity_uncompressed.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_segment>
    PARENT_SCOPE)
