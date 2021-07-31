add_library_unity(
  duckdb_storage_segment
  OBJECT
  numeric_constant.cpp
  numeric_uncompressed.cpp
  string_uncompressed.cpp
  validity_uncompressed.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_segment>
    PARENT_SCOPE)
