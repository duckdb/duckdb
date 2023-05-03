add_library_unity(
  duckdb_storage_statistics
  OBJECT
  base_statistics.cpp
  column_statistics.cpp
  distinct_statistics.cpp
  list_stats.cpp
  numeric_stats.cpp
  numeric_stats_union.cpp
  segment_statistics.cpp
  string_stats.cpp
  struct_stats.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_statistics>
    PARENT_SCOPE)
