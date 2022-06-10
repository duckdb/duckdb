add_library_unity(
  duckdb_storage_statistics
  OBJECT
  base_statistics.cpp
  column_statistics.cpp
  distinct_statistics.cpp
  list_statistics.cpp
  numeric_statistics.cpp
  segment_statistics.cpp
  string_statistics.cpp
  struct_statistics.cpp
  validity_statistics.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_statistics>
    PARENT_SCOPE)
