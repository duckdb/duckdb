add_library_unity(
  duckdb_storage_compression_roaring
  OBJECT
  common.cpp
  metadata.cpp
  analyze.cpp
  compress.cpp
  scan.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression_roaring>
    PARENT_SCOPE)
