add_library_unity(
  duckdb_storage_segment
  OBJECT
  constant_segment.cpp
  numeric_segment.cpp
  string_segment.cpp
  validity_segment.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_segment>
    PARENT_SCOPE)
